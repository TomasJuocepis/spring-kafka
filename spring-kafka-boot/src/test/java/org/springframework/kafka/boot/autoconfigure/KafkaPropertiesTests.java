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

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


/**
 * @author Gary Russell
 *
 */

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(KafkaPropertiesTests.Config.class)
@IntegrationTest({
	"spring.kafka.common.bootstrap-servers=foo:1234",
	"spring.kafka.common.extra.baz=qux",
	"spring.kafka.consumer.group-id=bar",
	"spring.kafka.consumer.enable-auto-commit=true",
	"spring.kafka.consumer.extra.fiz=buz",
	"spring.kafka.producer.batch-size=20",
	"spring.kafka.producer.extra.wam=tup",
	})
public class KafkaPropertiesTests {

	@Autowired
	private KafkaProperties props;

	@Autowired
	private ConsumerFactory<?, ?> consumerFactory;

	@Autowired
	private ProducerFactory<?, ?> producerFactory;

	@Test
	public void testConsumerProps() {
		KafkaProperties props = new KafkaProperties();
		props.getCommon().setBootstrapServers("foo:1234");
		props.getConsumer().setGroupId("bar");
		props.getConsumer().setEnableAutoCommit(true);
		// ...
		Map<String, Object> consumerProps = props.buildConsumerProperties();
		assertEquals("foo:1234", consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
		assertEquals("bar", consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG));
		assertEquals(true, consumerProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
		// ...
	}

	@Test
	public void testAll() {
		Map<String, Object> consumerProps = this.props.buildConsumerProperties();
		assertEquals("foo:1234", consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
		assertEquals("bar", consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG));
		assertEquals(true, consumerProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
		// ...
		assertEquals("qux", consumerProps.get("baz"));
		assertEquals("buz", consumerProps.get("fiz"));

		Map<String, Object> producerProps = this.props.buildProducerProperties();
		assertEquals("foo:1234", producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
		assertEquals(20, producerProps.get(ProducerConfig.BATCH_SIZE_CONFIG));
		// ...
		assertEquals("qux", producerProps.get("baz"));
		assertEquals("tup", producerProps.get("wam"));

		assertEquals(consumerProps, new DirectFieldAccessor(this.consumerFactory).getPropertyValue("configs"));
		assertEquals(producerProps, new DirectFieldAccessor(this.producerFactory).getPropertyValue("configs"));
	}

	@SpringBootApplication
	public static class Config {

	}

}
