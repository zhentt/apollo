package com.ctrip.framework.apollo.biz.grayReleaseRule;

import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleItemDTO;

import java.util.Set;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class GrayReleaseRuleCache {
  private long ruleId;

  /**
   * 相比 GrayReleaseRule 来说：
   *
   * 少了 appId + clusterName 字段，因为在 GrayReleaseRulesHolder 中，缓存 KEY 会根据需要包含这两个字段。
   * 多了 loadVersion 字段，用于记录 GrayReleaseRuleCache 的加载版本，用于自动过期逻辑。
   */

  private String branchName;
  private String namespaceName;
  private long releaseId;
  /**
   * 加载版本
   */
  private long loadVersion;
  private int branchStatus;
  private Set<GrayReleaseRuleItemDTO> ruleItems;

  public GrayReleaseRuleCache(long ruleId, String branchName, String namespaceName, long
      releaseId, int branchStatus, long loadVersion, Set<GrayReleaseRuleItemDTO> ruleItems) {
    this.ruleId = ruleId;
    this.branchName = branchName;
    this.namespaceName = namespaceName;
    this.releaseId = releaseId;
    this.branchStatus = branchStatus;
    this.loadVersion = loadVersion;
    this.ruleItems = ruleItems;
  }

  public long getRuleId() {
    return ruleId;
  }

  public Set<GrayReleaseRuleItemDTO> getRuleItems() {
    return ruleItems;
  }

  public String getBranchName() {
    return branchName;
  }

  public int getBranchStatus() {
    return branchStatus;
  }

  public long getReleaseId() {
    return releaseId;
  }

  public long getLoadVersion() {
    return loadVersion;
  }

  public void setLoadVersion(long loadVersion) {
    this.loadVersion = loadVersion;
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  // 匹配 clientAppId + clientIp
  public boolean matches(String clientAppId, String clientIp) {
    for (GrayReleaseRuleItemDTO ruleItem : ruleItems) {
      if (ruleItem.matches(clientAppId, clientIp)) {
        return true;
      }
    }
    return false;
  }
}
