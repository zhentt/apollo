package com.ctrip.framework.apollo.adminservice.controller;


import com.google.common.base.Splitter;

import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.message.MessageSender;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.service.NamespaceBranchService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.biz.service.ReleaseService;
import com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ReleaseDTO;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
public class ReleaseController {

  private static final Splitter RELEASES_SPLITTER = Splitter.on(",").omitEmptyStrings()
      .trimResults();


  @Autowired
  private ReleaseService releaseService;
  @Autowired
  private NamespaceService namespaceService;
  @Autowired
  private MessageSender messageSender;
  @Autowired
  private NamespaceBranchService namespaceBranchService;


  @RequestMapping(value = "/releases/{releaseId}", method = RequestMethod.GET)
  public ReleaseDTO get(@PathVariable("releaseId") long releaseId) {
    Release release = releaseService.findOne(releaseId);
    if (release == null) {
      throw new NotFoundException(String.format("release not found for %s", releaseId));
    }
    return BeanUtils.transfrom(ReleaseDTO.class, release);
  }

  @RequestMapping(value = "/releases", method = RequestMethod.GET)
  public List<ReleaseDTO> findReleaseByIds(@RequestParam("releaseIds") String releaseIds) {
    Set<Long> releaseIdSet = RELEASES_SPLITTER.splitToList(releaseIds).stream().map(Long::parseLong)
        .collect(Collectors.toSet());

    List<Release> releases = releaseService.findByReleaseIds(releaseIdSet);

    return BeanUtils.batchTransform(ReleaseDTO.class, releases);
  }

  @RequestMapping(value = "/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/releases/all", method = RequestMethod.GET)
  public List<ReleaseDTO> findAllReleases(@PathVariable("appId") String appId,
                                          @PathVariable("clusterName") String clusterName,
                                          @PathVariable("namespaceName") String namespaceName,
                                          Pageable page) {
    List<Release> releases = releaseService.findAllReleases(appId, clusterName, namespaceName, page);
    return BeanUtils.batchTransform(ReleaseDTO.class, releases);
  }


  @RequestMapping(value = "/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/releases/active", method = RequestMethod.GET)
  public List<ReleaseDTO> findActiveReleases(@PathVariable("appId") String appId,
                                             @PathVariable("clusterName") String clusterName,
                                             @PathVariable("namespaceName") String namespaceName,
                                             Pageable page) {
    List<Release> releases = releaseService.findActiveReleases(appId, clusterName, namespaceName, page);
    return BeanUtils.batchTransform(ReleaseDTO.class, releases);
  }

  @RequestMapping(value = "/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/releases/latest", method = RequestMethod.GET)
  public ReleaseDTO getLatest(@PathVariable("appId") String appId,
                              @PathVariable("clusterName") String clusterName,
                              @PathVariable("namespaceName") String namespaceName) {
    Release release = releaseService.findLatestActiveRelease(appId, clusterName, namespaceName);
    return BeanUtils.transfrom(ReleaseDTO.class, release);
  }

  @Transactional
  @RequestMapping(path = "/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/releases", method = RequestMethod.POST)
  public ReleaseDTO publish(@PathVariable("appId") String appId,
                            @PathVariable("clusterName") String clusterName,
                            @PathVariable("namespaceName") String namespaceName,
                            @RequestParam("name") String releaseName,
                            @RequestParam(name = "comment", required = false) String releaseComment,
                            @RequestParam("operator") String operator,
                            @RequestParam(name = "isEmergencyPublish", defaultValue = "false") boolean isEmergencyPublish) {
    Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);
    if (namespace == null) {
      throw new NotFoundException(String.format("Could not find namespace for %s %s %s", appId,
                                                clusterName, namespaceName));
    }
    Release release = releaseService.publish(namespace, releaseName, releaseComment, operator, isEmergencyPublish);

    //send release message
    // 获得 Cluster 名
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);
    String messageCluster;
    //  有父 Namespace ，说明是灰度发布，使用父 Namespace 的集群名
    if (parentNamespace != null) {
      messageCluster = parentNamespace.getClusterName();
    } else {
      // 使用请求的 ClusterName
      messageCluster = clusterName;
    }
    // 发送 Release 消息
    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, messageCluster, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);
    return BeanUtils.transfrom(ReleaseDTO.class, release);
  }


  /**
   * merge branch items to master and publish master
   *
   * @return published result
   */
  @Transactional
  @RequestMapping(path = "/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/updateAndPublish", method = RequestMethod.POST)
  public ReleaseDTO updateAndPublish(@PathVariable("appId") String appId,
                                     @PathVariable("clusterName") String clusterName,
                                     @PathVariable("namespaceName") String namespaceName,
                                     @RequestParam("releaseName") String releaseName,
                                     @RequestParam("branchName") String branchName,
                                     @RequestParam(value = "deleteBranch", defaultValue = "true") boolean deleteBranch,
                                     @RequestParam(name = "releaseComment", required = false) String releaseComment,
                                     @RequestParam(name = "isEmergencyPublish", defaultValue = "false") boolean isEmergencyPublish,
                                     @RequestBody ItemChangeSets changeSets) {
    // 获得 Namespace
    Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);
    if (namespace == null) {
      throw new NotFoundException(String.format("Could not find namespace for %s %s %s", appId,
                                                clusterName, namespaceName));
    }

    // 合并子 Namespace 变更的配置 Map 到父 Namespace ，并进行一次 Release
    Release release = releaseService.mergeBranchChangeSetsAndRelease(namespace, branchName, releaseName,
                                                                     releaseComment, isEmergencyPublish, changeSets);

    // 若需要删除子 Namespace ，则进行删除
    if (deleteBranch) {
      namespaceBranchService.deleteBranch(appId, clusterName, namespaceName, branchName,
                                          NamespaceBranchStatus.MERGED, changeSets.getDataChangeLastModifiedBy());
    }

    // 发送 Release 消息
    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);

    // 将 Release 转换成 ReleaseDTO 对象
    return BeanUtils.transfrom(ReleaseDTO.class, release);

  }

  @Transactional
  @RequestMapping(path = "/releases/{releaseId}/rollback", method = RequestMethod.PUT)
  public void rollback(@PathVariable("releaseId") long releaseId,
                       @RequestParam("operator") String operator) {

    Release release = releaseService.rollback(releaseId, operator);

    String appId = release.getAppId();
    String clusterName = release.getClusterName();
    String namespaceName = release.getNamespaceName();
    //send release message
    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);
  }

}
