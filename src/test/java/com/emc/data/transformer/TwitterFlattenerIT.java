package com.emc.data.transformer;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.FileCopyUtils;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=TwitterFlattenerConfiguration.class)
public class TwitterFlattenerIT {

	@Autowired
	private DirectChannel input;
	
	@Autowired
	private AbstractSubscribableChannel output;
	
	private String tweet;
	
	@Before
	public void before() throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		FileCopyUtils.copy(this.getClass().getResourceAsStream("/tweet.json"), bos);
		tweet = new String(bos.toByteArray());
	}
	
	
	@Test
	public void testFlattenTwitter() throws Exception {
		//build the message
		Message<?> message = MessageBuilder.withPayload(tweet).build();
		//subscribe
		TestMessageHandler handler = new TestMessageHandler();
		output.subscribe(handler);
		//send
		input.send(message);
		//wait
		while (handler.message == null) {
			Thread.sleep(5 * 100);//wait half a second
		}//end while
		//extract and test
		assertTrue(handler.message.getPayload().toString().contains("|#BahrainLive #Bahrain #sudia #kuwait #Qatar #Qatar2022 #Dubai #oman #ksa #emarate #uae #Iraq #Egypt #jordan #yame http://t.co/ZzjPR2Zgbb|"));
		assertTrue(handler.message.getPayload().toString().contains("|BahrainLive,Bahrain,sudia,kuwait,Qatar,Qatar2022,Dubai,oman,ksa,emarate,uae,Iraq,Egypt,jordan,yame|"));
	}

	
	class TestMessageHandler implements MessageHandler {

		Message<?> message = null;
		
		@Override
		public void handleMessage(Message<?> message) throws MessagingException {
			this.message = message;
		}
		
	}
}
