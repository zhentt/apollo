package com.ctrip.framework.apollo.portal.service;

import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleDTO;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.dto.NamespaceDTO;
import com.ctrip.framework.apollo.common.dto.ReleaseDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.core.enums.Env;
import com.ctrip.framework.apollo.portal.api.AdminServiceAPI;
import com.ctrip.framework.apollo.portal.component.ItemsComparator;
import com.ctrip.framework.apollo.portal.constant.TracerEventType;
import com.ctrip.framework.apollo.portal.entity.bo.NamespaceBO;
import com.ctrip.framework.apollo.portal.spi.UserInfoHolder;
import com.ctrip.framework.apollo.tracer.Tracer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;

@Service
public class NamespaceBranchService {

  @Autowired
  private ItemsComparator itemsComparator;
  @Autowired
  private UserInfoHolder userInfoHolder;
  @Autowired
  private NamespaceService namespaceService;
  @Autowired
  private ItemService itemService;
  @Autowired
  private AdminServiceAPI.NamespaceBranchAPI namespaceBranchAPI;
  @Autowired
  private ReleaseService releaseService;


  @Transactional
  public NamespaceDTO createBranch(String appId, Env env, String parentClusterName, String namespaceName) {
    // 创建 Namespace 分支
    NamespaceDTO createdBranch = namespaceBranchAPI.createBranch(appId, env, parentClusterName, namespaceName,
                                                                 userInfoHolder.getUser().getUserId());

    Tracer.logEvent(TracerEventType.CREATE_GRAY_RELEASE, String.format("%s+%s+%s+%s", appId, env, parentClusterName,
                                                                 namespaceName));
    return createdBranch;

  }

  public GrayReleaseRuleDTO findBranchGrayRules(String appId, Env env, String clusterName,
                                                String namespaceName, String branchName) {
    return namespaceBranchAPI.findBranchGrayRules(appId, env, clusterName, namespaceName, branchName);

  }

  public void updateBranchGrayRules(String appId, Env env, String clusterName, String namespaceName,
                                    String branchName, GrayReleaseRuleDTO rules) {

    // 设置 GrayReleaseRuleDTO 的创建和修改人为当前管理员
    String operator = userInfoHolder.getUser().getUserId();
    rules.setDataChangeCreatedBy(operator);
    rules.setDataChangeLastModifiedBy(operator);

    // 更新 Namespace 分支的灰度规则
    namespaceBranchAPI.updateBranchGrayRules(appId, env, clusterName, namespaceName, branchName, rules);

    Tracer.logEvent(TracerEventType.UPDATE_GRAY_RELEASE_RULE,
                 String.format("%s+%s+%s+%s", appId, env, clusterName, namespaceName));
  }

  public void deleteBranch(String appId, Env env, String clusterName, String namespaceName,
                           String branchName) {

    String operator = userInfoHolder.getUser().getUserId();

    namespaceBranchAPI.deleteBranch(appId, env, clusterName, namespaceName, branchName, operator);

    Tracer.logEvent(TracerEventType.DELETE_GRAY_RELEASE,
                 String.format("%s+%s+%s+%s", appId, env, clusterName, namespaceName));
  }

  /**
   * 调用 Admin Service API ，合并子 Namespace 变更的配置 Map 到父 Namespace ，并进行一次 Release 。
   * @param appId
   * @param env
   * @param clusterName
   * @param namespaceName
   * @param branchName
   * @param title
   * @param comment
   * @param isEmergencyPublish
   * @param deleteBranch
   * @return
   */
  public ReleaseDTO merge(String appId, Env env, String clusterName, String namespaceName,
                          String branchName, String title, String comment,
                          boolean isEmergencyPublish, boolean deleteBranch) {

    // 计算变化的 Item 集合
    ItemChangeSets changeSets = calculateBranchChangeSet(appId, env, clusterName, namespaceName, branchName);

    // 合并子 Namespace 变更的配置 Map 到父 Namespace ，并进行一次 Release
    ReleaseDTO mergedResult =
        releaseService.updateAndPublish(appId, env, clusterName, namespaceName, title, comment,
                                        branchName, isEmergencyPublish, deleteBranch, changeSets);

    Tracer.logEvent(TracerEventType.MERGE_GRAY_RELEASE,
                 String.format("%s+%s+%s+%s", appId, env, clusterName, namespaceName));

    return mergedResult;
  }

  private ItemChangeSets calculateBranchChangeSet(String appId, Env env, String clusterName, String namespaceName,
                                                  String branchName) {
    // 获得父 NamespaceBO 对象
    NamespaceBO parentNamespace = namespaceService.loadNamespaceBO(appId, env, clusterName, namespaceName);

    // 若父 Namespace 不存在，抛出 BadRequestException 异常。
    if (parentNamespace == null) {
      throw new BadRequestException("base namespace not existed");
    }

    // 若父 Namespace 有配置项的变更，不允许合并。因为，可能存在冲突。
    if (parentNamespace.getItemModifiedCnt() > 0) {
      throw new BadRequestException("Merge operation failed. Because master has modified items");
    }

    // 获得父 Namespace 的 Item 数组
    List<ItemDTO> masterItems = itemService.findItems(appId, env, clusterName, namespaceName);

    // 获得子 Namespace 的 Item 数组
    List<ItemDTO> branchItems = itemService.findItems(appId, env, branchName, namespaceName);

    // 计算变化的 Item 集合
    ItemChangeSets changeSets = itemsComparator.compareIgnoreBlankAndCommentItem(parentNamespace.getBaseInfo().getId(),
                                                                                 masterItems, branchItems);
    // 设置 `ItemChangeSets.deleteItem` 为空。因为子 Namespace 从父 Namespace 继承配置，但是实际自己没有那些配置项，所以如果不清空，会导致这些配置项被删除。
    changeSets.setDeleteItems(Collections.emptyList());
    // 设置 `ItemChangeSets.dataChangeLastModifiedBy` 为当前管理员
    changeSets.setDataChangeLastModifiedBy(userInfoHolder.getUser().getUserId());
    return changeSets;
  }

  public NamespaceDTO findBranchBaseInfo(String appId, Env env, String clusterName, String namespaceName) {
    return namespaceBranchAPI.findBranch(appId, env, clusterName, namespaceName);
  }

  public NamespaceBO findBranch(String appId, Env env, String clusterName, String namespaceName) {
    NamespaceDTO namespaceDTO = findBranchBaseInfo(appId, env, clusterName, namespaceName);
    if (namespaceDTO == null) {
      return null;
    }
    return namespaceService.loadNamespaceBO(appId, env, namespaceDTO.getClusterName(), namespaceName);
  }

}
