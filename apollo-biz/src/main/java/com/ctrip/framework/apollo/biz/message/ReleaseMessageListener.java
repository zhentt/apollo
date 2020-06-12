package com.ctrip.framework.apollo.biz.message;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface ReleaseMessageListener {
  /**
   * 处理 ReleaseMessage
   * @param message
   * @param channel
   */
  void handleMessage(ReleaseMessage message, String channel);
}
