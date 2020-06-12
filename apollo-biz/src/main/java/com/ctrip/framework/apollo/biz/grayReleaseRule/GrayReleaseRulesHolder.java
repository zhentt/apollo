package com.ctrip.framework.apollo.biz.grayReleaseRule;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.ReleaseMessageListener;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.repository.GrayReleaseRuleRepository;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleItemDTO;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public class GrayReleaseRulesHolder implements ReleaseMessageListener, InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(GrayReleaseRulesHolder.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  private static final Splitter STRING_SPLITTER =
      Splitter.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR).omitEmptyStrings();

  @Autowired
  private GrayReleaseRuleRepository grayReleaseRuleRepository;
  @Autowired
  private BizConfig bizConfig;

  /**
   * 数据库扫描频率，单位：秒
   */
  private int databaseScanInterval;
  /**
   * ExecutorService 对象
   */
  private ScheduledExecutorService executorService;
  /**
   * GrayReleaseRuleCache 缓存
   *
   * KEY：configAppId+configCluster+configNamespace ，通过 {@link #assembleGrayReleaseRuleKey(String, String, String)} 生成
   *      注意，KEY 中不包含 BranchName
   * VALUE：GrayReleaseRuleCache 数组
   */
  //store configAppId+configCluster+configNamespace -> GrayReleaseRuleCache map
  private Multimap<String, GrayReleaseRuleCache> grayReleaseRuleCache;
  /**
   * GrayReleaseRuleCache 缓存2
   *
   * KEY：clientAppId+clientNamespace+ip ，通过 {@link #assembleReversedGrayReleaseRuleKey(String, String, String)} 生成
   *      注意，KEY 中不包含 ClusterName
   * VALUE：{@link GrayReleaseRule} 数组
   */
  //store clientAppId+clientNamespace+ip -> ruleId map
  private Multimap<String, Long> reversedGrayReleaseRuleCache;
  /**
   * 加载版本号
   */
  //an auto increment version to indicate the age of rules
  private AtomicLong loadVersion;

  public GrayReleaseRulesHolder() {
    loadVersion = new AtomicLong();
    grayReleaseRuleCache = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    reversedGrayReleaseRuleCache = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    executorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
        .create("GrayReleaseRulesHolder", true));
  }

  /**
   * 通过 Spring 调用，初始化 Scan 任务。
   * @throws Exception
   */
  @Override
  public void afterPropertiesSet() throws Exception {
    // 从 ServerConfig 中，读取任务的周期配置
    populateDataBaseInterval();
    // 初始拉取 GrayReleaseRuleCache 到缓存
    //force sync load for the first time
    periodicScanRules();
    // 定时拉取 GrayReleaseRuleCache 到缓存
    executorService.scheduleWithFixedDelay(this::periodicScanRules,
        getDatabaseScanIntervalSecond(), getDatabaseScanIntervalSecond(), getDatabaseScanTimeUnit()
    );
  }

  /**
   * 基于 ReleaseMessage 近实时通知，更新缓存。
   * @param message
   * @param channel
   */
  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    logger.info("message received - channel: {}, message: {}", channel, message);
    String releaseMessage = message.getMessage();
    // 只处理 APOLLO_RELEASE_TOPIC 的消息
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(releaseMessage)) {
      return;
    }
    // 获得 appId cluster namespace 参数
    List<String> keys = STRING_SPLITTER.splitToList(releaseMessage);
    //message should be appId+cluster+namespace
    if (keys.size() != 3) {
      logger.error("message format invalid - {}", releaseMessage);
      return;
    }
    String appId = keys.get(0);
    String cluster = keys.get(1);
    String namespace = keys.get(2);

    // 获得对应的 GrayReleaseRule 数组
    List<GrayReleaseRule> rules = grayReleaseRuleRepository
        .findByAppIdAndClusterNameAndNamespaceName(appId, cluster, namespace);

    // 合并到 GrayReleaseRule 缓存中
    mergeGrayReleaseRules(rules);
  }

  /**
   * 初始拉取 GrayReleaseRuleCache 到缓存。
   */
  private void periodicScanRules() {
    Transaction transaction = Tracer.newTransaction("Apollo.GrayReleaseRulesScanner",
        "scanGrayReleaseRules");
    try {
      // 递增加载版本号
      loadVersion.incrementAndGet();
      // 从数据卷库中，扫描所有 GrayReleaseRules ，并合并到缓存中
      scanGrayReleaseRules();
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      transaction.setStatus(ex);
      logger.error("Scan gray release rule failed", ex);
    } finally {
      transaction.complete();
    }
  }

  /**
   * 若匹配上灰度规则，返回对应的 Release 编号。
   * @param clientAppId
   * @param clientIp
   * @param configAppId
   * @param configCluster
   * @param configNamespaceName
   * @return
   */
  public Long findReleaseIdFromGrayReleaseRule(String clientAppId, String clientIp, String
      configAppId, String configCluster, String configNamespaceName) {
    // 判断 grayReleaseRuleCache 中是否存在
    String key = assembleGrayReleaseRuleKey(configAppId, configCluster, configNamespaceName);
    if (!grayReleaseRuleCache.containsKey(key)) {
      return null;
    }
    // 循环 GrayReleaseRuleCache 数组，获得匹配的 Release 编号
    //create a new list to avoid ConcurrentModificationException
    List<GrayReleaseRuleCache> rules = Lists.newArrayList(grayReleaseRuleCache.get(key));
    for (GrayReleaseRuleCache rule : rules) {
      // 校验 GrayReleaseRuleCache 对应的子 Namespace 的状态是否为有效
      //check branch status
      if (rule.getBranchStatus() != NamespaceBranchStatus.ACTIVE) {
        continue;
      }
      // 是否匹配灰度规则。若是，则返回。
      if (rule.matches(clientAppId, clientIp)) {
        return rule.getReleaseId();
      }
    }
    return null;
  }

  /**
   * Check whether there are gray release rules for the clientAppId, clientIp, namespace
   * combination. Please note that even there are gray release rules, it doesn't mean it will always
   * load gray releases. Because gray release rules actually apply to one more dimension - cluster.
   */
  /**
   * 针对 clientAppId + clientIp + namespaceName ，校验是否有灰度规则。
   * 请注意，即使返回 true ，也不意味着调用方能加载到灰度发布的配置。
   * 因为，reversedGrayReleaseRuleCache 的 KEY 不包含 branchName ，所以 reversedGrayReleaseRuleCache 的 VALUE 为多个 branchName 的 Release 编号的集合。
   * 为什么不包含 branchName 呢？
   * AbstractConfigService 中，#loadConfig(...) 方法中，是按照集群的优先级加载
   * 
   * @param clientAppId
   * @param clientIp
   * @param namespaceName
   * @return
   */
  public boolean hasGrayReleaseRule(String clientAppId, String clientIp, String namespaceName) {
    return reversedGrayReleaseRuleCache.containsKey(assembleReversedGrayReleaseRuleKey(clientAppId,
        namespaceName, clientIp)) || reversedGrayReleaseRuleCache.containsKey
        (assembleReversedGrayReleaseRuleKey(clientAppId, namespaceName, GrayReleaseRuleItemDTO
            .ALL_IP));
  }

  private void scanGrayReleaseRules() {
    long maxIdScanned = 0;
    boolean hasMore = true;

    // 循环顺序分批加载 GrayReleaseRule ，直到结束或者线程打断
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      // 顺序分批加载 GrayReleaseRule 500 条
      List<GrayReleaseRule> grayReleaseRules = grayReleaseRuleRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);
      if (CollectionUtils.isEmpty(grayReleaseRules)) {
        break;
      }
      // 合并到 GrayReleaseRule 缓存
      mergeGrayReleaseRules(grayReleaseRules);
      // 获得新的 maxIdScanned ，取最后一条记录
      int rulesScanned = grayReleaseRules.size();
      maxIdScanned = grayReleaseRules.get(rulesScanned - 1).getId();
      // batch is 500
      // 若拉取不足 500 条，说明无 GrayReleaseRule 了
      hasMore = rulesScanned == 500;
    }
  }

  /**
   * 合并 GrayReleaseRule 到缓存中。
   * @param grayReleaseRules
   */
  private void mergeGrayReleaseRules(List<GrayReleaseRule> grayReleaseRules) {
    if (CollectionUtils.isEmpty(grayReleaseRules)) {
      return;
    }
    // !!! 注意，下面说的“老”，指的是已经在缓存中，但是实际不一定“老”。
    for (GrayReleaseRule grayReleaseRule : grayReleaseRules) {
      // 无对应的 Release 编号，记未灰度发布，则无视
      if (grayReleaseRule.getReleaseId() == null || grayReleaseRule.getReleaseId() == 0) {
        //filter rules with no release id, i.e. never released
        continue;
      }
      // 创建 `grayReleaseRuleCache` 的 KEY
      String key = assembleGrayReleaseRuleKey(grayReleaseRule.getAppId(), grayReleaseRule
          .getClusterName(), grayReleaseRule.getNamespaceName());
      // 从缓存 `grayReleaseRuleCache` 读取，并创建数组，避免并发
      //create a new list to avoid ConcurrentModificationException
      List<GrayReleaseRuleCache> rules = Lists.newArrayList(grayReleaseRuleCache.get(key));
      // 获得子 Namespace 对应的老的 GrayReleaseRuleCache 对象
      GrayReleaseRuleCache oldRule = null;
      for (GrayReleaseRuleCache ruleCache : rules) {
        if (ruleCache.getBranchName().equals(grayReleaseRule.getBranchName())) {
          oldRule = ruleCache;
          break;
        }
      }

      // 忽略，若不存在老的 GrayReleaseRuleCache ，并且当前 GrayReleaseRule 对应的分支不处于激活( 有效 )状态
      //if old rule is null and new rule's branch status is not active, ignore
      if (oldRule == null && grayReleaseRule.getBranchStatus() != NamespaceBranchStatus.ACTIVE) {
        continue;
      }

      // 若新的 GrayReleaseRule 为新增或更新，进行缓存更新
      //use id comparison to avoid synchronization
      if (oldRule == null || grayReleaseRule.getId() > oldRule.getRuleId()) {
        // 添加新的 GrayReleaseRuleCache 到缓存中
        addCache(key, transformRuleToRuleCache(grayReleaseRule));
        // 移除老的 GrayReleaseRuleCache 出缓存中
        if (oldRule != null) {
          removeCache(key, oldRule);
        }
      } else {
        // 老的 GrayReleaseRuleCache 对应的分支处于激活( 有效 )状态，更新加载版本号。
        // 例如，定时轮询，有可能，早于 `#handleMessage(...)` 拿到对应的新的 GrayReleaseRule 记录，那么此时规则编号是相等的，不符合上面的条件，但是符合这个条件。
        // 再例如，两次定时轮询，第二次和第一次的规则编号是相等的，不符合上面的条件，但是符合这个条件。
        if (oldRule.getBranchStatus() == NamespaceBranchStatus.ACTIVE) {
          //update load version
          oldRule.setLoadVersion(loadVersion.get());
          // 保留两轮，
          // 适用于，`GrayReleaseRule.branchStatus` 为 DELETED 或 MERGED 的情况。
          /**
           * 例如，灰度全量发布时，会添加 GrayReleaseRule.branchStatus 为 MERGED 到缓存中。保留两轮，进行移除出缓存。
           *
           */
        } else if ((loadVersion.get() - oldRule.getLoadVersion()) > 1) {
          //remove outdated inactive branch rule after 2 update cycles
          removeCache(key, oldRule);
        }
      }
    }
  }

  /**
   * 添加新的 GrayReleaseRuleCache 到缓存中。
   * @param key
   * @param ruleCache
   */
  private void addCache(String key, GrayReleaseRuleCache ruleCache) {
    // 添加到 reversedGrayReleaseRuleCache 中
    // 为什么这里判断状态？因为删除灰度，或者灰度全量发布的情况下，是无效的，所以不添加到 reversedGrayReleaseRuleCache 中
    if (ruleCache.getBranchStatus() == NamespaceBranchStatus.ACTIVE) {
      for (GrayReleaseRuleItemDTO ruleItemDTO : ruleCache.getRuleItems()) {
        for (String clientIp : ruleItemDTO.getClientIpList()) {
          reversedGrayReleaseRuleCache.put(assembleReversedGrayReleaseRuleKey(ruleItemDTO
              .getClientAppId(), ruleCache.getNamespaceName(), clientIp), ruleCache.getRuleId());
        }
      }
    }
    // 添加到 grayReleaseRuleCache
    // 这里为什么可以添加？因为添加到 grayReleaseRuleCache 中是个对象，可以判断状态
    grayReleaseRuleCache.put(key, ruleCache);
  }

  /**
   * 移除老 的 GrayReleaseRuleCache 出缓存。
   * @param key
   * @param ruleCache
   */
  private void removeCache(String key, GrayReleaseRuleCache ruleCache) {
    // 移除出 grayReleaseRuleCache
    grayReleaseRuleCache.remove(key, ruleCache);
    // 移除出 reversedGrayReleaseRuleCache
    for (GrayReleaseRuleItemDTO ruleItemDTO : ruleCache.getRuleItems()) {
      for (String clientIp : ruleItemDTO.getClientIpList()) {
        reversedGrayReleaseRuleCache.remove(assembleReversedGrayReleaseRuleKey(ruleItemDTO
            .getClientAppId(), ruleCache.getNamespaceName(), clientIp), ruleCache.getRuleId());
      }
    }
  }

  /**
   * 将 GrayReleaseRule 转换成 GrayReleaseRuleCache 对象。
   * @param grayReleaseRule
   * @return
   */
  private GrayReleaseRuleCache transformRuleToRuleCache(GrayReleaseRule grayReleaseRule) {
    // 转换出 GrayReleaseRuleItemDTO 数组
    Set<GrayReleaseRuleItemDTO> ruleItems;
    try {
      ruleItems = GrayReleaseRuleItemTransformer.batchTransformFromJSON(grayReleaseRule.getRules());
    } catch (Throwable ex) {
      ruleItems = Sets.newHashSet();
      Tracer.logError(ex);
      logger.error("parse rule for gray release rule {} failed", grayReleaseRule.getId(), ex);
    }

    // 创建 GrayReleaseRuleCache 对象，并返回
    GrayReleaseRuleCache ruleCache = new GrayReleaseRuleCache(grayReleaseRule.getId(),
        grayReleaseRule.getBranchName(), grayReleaseRule.getNamespaceName(), grayReleaseRule
        .getReleaseId(), grayReleaseRule.getBranchStatus(), loadVersion.get(), ruleItems);

    return ruleCache;
  }

  /**
   * 从 ServerConfig 中，读取定时任务的周期配置。
   */
  private void populateDataBaseInterval() {
    // "apollo.gray-release-rule-scan.interval" ，默认为 60 。
    databaseScanInterval = bizConfig.grayReleaseRuleScanInterval();
  }

  private int getDatabaseScanIntervalSecond() {
    return databaseScanInterval;
  }

  private TimeUnit getDatabaseScanTimeUnit() {
    return TimeUnit.SECONDS;
  }

  private String assembleGrayReleaseRuleKey(String configAppId, String configCluster, String
      configNamespaceName) {
    return STRING_JOINER.join(configAppId, configCluster, configNamespaceName);
  }

  private String assembleReversedGrayReleaseRuleKey(String clientAppId, String
      clientNamespaceName, String clientIp) {
    return STRING_JOINER.join(clientAppId, clientNamespaceName, clientIp);
  }

}
