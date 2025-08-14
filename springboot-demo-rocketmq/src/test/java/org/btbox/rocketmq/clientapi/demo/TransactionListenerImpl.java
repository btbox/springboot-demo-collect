package org.btbox.rocketmq.clientapi.demo;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class TransactionListenerImpl implements TransactionListener {
	/**
	 * 执行本地事务
	 * 当事务half消息发送成功，这个方法将被执行
	 * 事务的half消息是发到 RMQ_SYS_TRANS_OP_HALF_TOPIC 的topic中
	 *
	 * @param msg 消息
	 * @param arg arg 自定义业务参数
	 * @return {@link LocalTransactionState}
	 */
	@Override
	public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
		String tags = msg.getTags();
		System.out.println("============执行executeLocalTransaction方法，;消息内容是="+new String(msg.getBody()));
		if (StringUtils.contains(tags, "tagA")) {
			return LocalTransactionState.COMMIT_MESSAGE;
		} else if (StringUtils.contains(tags, "tagB")) {
			return LocalTransactionState.ROLLBACK_MESSAGE;
		}
		return LocalTransactionState.UNKNOW;
	}
	/**
	 * 检查本地事务
	 * 回查本地事务状态，当half消息没响应时调用。
	 * 回查状态 15次都是UNKNOW则直接抛弃该消息
	 * @param msg 消息
	 * @return {@link LocalTransactionState}
	 */
	@Override
	public LocalTransactionState checkLocalTransaction(MessageExt msg) {
		String tags = msg.getTags();
		System.out.println("============执行checkLocalTransaction方法，;消息内容是="+new String(msg.getBody()));
		if (StringUtils.contains(tags, "tagC")) {
			return LocalTransactionState.COMMIT_MESSAGE;
		} else if (StringUtils.contains(tags, "tagD")) {
			return LocalTransactionState.ROLLBACK_MESSAGE;
		}
		return LocalTransactionState.UNKNOW;
	}
}