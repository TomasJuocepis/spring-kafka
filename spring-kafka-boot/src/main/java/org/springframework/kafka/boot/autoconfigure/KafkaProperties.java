/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.kafka.boot.autoconfigure;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Gary Russell
 *
 */
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaProperties {

	private final Common common = new Common();

	private final Consumer consumer = new Consumer();

	private final Producer producer = new Producer();


	public Common getCommon() {
		return this.common;
	}

	public Consumer getConsumer() {
		return this.consumer;
	}

	public Producer getProducer() {
		return this.producer;
	}

	public Map<String, Object> buildConsumerProperties() {
		Map<String, Object> props = new HashMap<String, Object>();
		if (this.common.bootstrapServers != null) {
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.common.bootstrapServers);
		}
		if (this.consumer.groupId != null) {
			props.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumer.groupId);
		}
		if (this.consumer.enableAutoCommit != null) {
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, this.consumer.enableAutoCommit);
		}
		// ...
		for (Entry<String, Object> entry : this.common.extra.entrySet()) {
			props.put(entry.getKey(), entry.getValue());
		}
		for (Entry<String, Object> entry : this.consumer.extra.entrySet()) {
			props.put(entry.getKey(), entry.getValue());
		}
		return props;
	}

	public Map<String, Object> buildProducerProperties() {
		Map<String, Object> props = new HashMap<String, Object>();
		if (this.common.bootstrapServers != null) {
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.common.bootstrapServers);
		}
		if (this.producer.batchSize != null) {
			props.put(ProducerConfig.BATCH_SIZE_CONFIG, this.producer.batchSize);
		}
		// ...
		for (Entry<String, Object> entry : this.common.extra.entrySet()) {
			props.put(entry.getKey(), entry.getValue());
		}
		for (Entry<String, Object> entry : this.producer.extra.entrySet()) {
			props.put(entry.getKey(), entry.getValue());
		}
		return props;
	}

	public static class Common {

		private String bootstrapServers;

		// ...

		private final Map<String, Object> extra = new HashMap<>();

		public String getBootstrapServers() {
			return this.bootstrapServers;
		}

		public void setBootstrapServers(String bootstrapServers) {
			this.bootstrapServers = bootstrapServers;
		}

		public Map<String, Object> getExtra() {
			return this.extra;
		}

	}

	public static class Consumer {

		private String groupId;

		private Boolean enableAutoCommit;

		// ...

		private final Map<String, Object> extra = new HashMap<>();

		public String getGroupId() {
			return this.groupId;
		}

		public void setGroupId(String groupId) {
			this.groupId = groupId;
		}

		public Boolean getEnableAutoCommit() {
			return this.enableAutoCommit;
		}

		public void setEnableAutoCommit(Boolean enableAutoCommit) {
			this.enableAutoCommit = enableAutoCommit;
		}

		public Map<String, Object> getExtra() {
			return this.extra;
		}

	}

	public static class Producer {

		private Integer batchSize;

		// ...

		private final Map<String, Object> extra = new HashMap<>();

		public Integer getBatchSize() {
			return this.batchSize;
		}

		public void setBatchSize(Integer batchSize) {
			this.batchSize = batchSize;
		}

		public Map<String, Object> getExtra() {
			return this.extra;
		}
	}

}
