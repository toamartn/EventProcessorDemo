package com.mss.eventhubs.demo.eventprocessor;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class EventProcessorDemo{
	
    public static void main(String args[]) throws InterruptedException, ExecutionException{
    	
    	String consumerGroupName = "$Default";	//	as is let it be default
    	String namespaceName = "mss****";	//	event hubs / Event Hubs Namespace
    	String eventHubName = "mss*****";	//	name of event hub
    	String sasKeyName = "SAS***";	//	create Shared access policies under event hub. Send/listen or both.
    	String sasKey = "***********";	//	shared access policies key
    	
    	/// storageConnectionString -> Storage account -> Access Keys default two keys key1 and key2 we get. use connection string
    	String storageConnectionString = "*******";
    	String storageContainerName = "***";	//	Storage account name
    	String hostNamePrefix = "***";	//	any name it will print something similar to.....  SAMPLE: Partition 1 batch size was 1 for host MSSHostNamePrefix-c2abce28-651b-4926-9ec5-0582f47b8b73
    	
    	ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder()
    			.setNamespaceName(namespaceName)
    			.setEventHubName(eventHubName)
    			.setSasKeyName(sasKeyName)
    			.setSasKey(sasKey);
    	
		EventProcessorHost host = new EventProcessorHost(
				EventProcessorHost.createHostName(hostNamePrefix),	//	 The host name, which identifies the instance of EventProcessorHost, must be unique. we can use a plain UUID also
				eventHubName,
				consumerGroupName,
				eventHubConnectionString.toString(),
				storageConnectionString,
				storageContainerName);
		
		/* 
		 * Registering an event processor class with an instance of EventProcessorHost starts event processing. 
			The host instance obtains leases on some partitions of the Event Hub. For each leased partition, the host instance
			creates an instance of the provided event processor class, then receives events from that partition and passes them to
			the event processor instance.
		
			There are two error notification systems in EventProcessorHost. Notification of errors tied to a particular partition,
			such as a receiver failing, are delivered to the event processor instance for that partition via the onError method.
		
			Notification of errors not tied to a particular partition, such as initialization failures, are delivered to a general
			notification handler that is specified via an EventProcessorOptions object. You are not required to provide such a
			notification handler, but if you don't, then you may not know that certain errors have occurred.
		*/
		
		System.out.println("Registering host named " + host.getHostName());
		
		EventProcessorOptions options = new EventProcessorOptions();
		options.setExceptionNotification(new ErrorNotificationHandler());

		host.registerEventProcessor(EventProcessor.class, options).whenComplete((unused, e) ->
				{
					if (e != null){
						System.out.println("Failure while registering: " + e.toString());
						if (e.getCause() != null){
							System.out.println("Inner exception: " + e.getCause().toString());
						}
					}
				}
			).thenAccept((unused) ->	//	executed only if the registerEventProcessor succeeded. 
				{
					System.out.println("Press enter to stop.");
					try{
						System.in.read();
					}catch (Exception e){
						System.out.println("Keyboard read failed: " + e.toString());
					}
				}
		).thenCompose((unused) ->	// executed only if registerEventProcessor succeeded.
			{
				return host.unregisterEventProcessor();
			}
		).exceptionally((e) ->{
				System.out.println("Failure while unregistering: " + e.toString());
				if (e.getCause() != null){
					System.out.println("Inner exception: " + e.getCause().toString());
				}
				return null;
			}
		).get(); // Wait for everything to finish before exiting main!
        System.out.println("End of sample");
    }
    
    public static class ErrorNotificationHandler implements Consumer<ExceptionReceivedEventArgs>{
		@Override
		public void accept(ExceptionReceivedEventArgs t){
			System.out.println("SAMPLE: Host " + t.getHostname() + " received general error notification during " + t.getAction() + ": " + t.getException().toString());
		}
    }

    public static class EventProcessor implements IEventProcessor{
    	
    	private int checkpointBatchingCount = 0;

    	@Override
        public void onOpen(PartitionContext context) throws Exception{
        	System.out.println("SAMPLE: Partition " + context.getPartitionId() + " is opening");
        }

    	@Override
        public void onClose(PartitionContext context, CloseReason reason) throws Exception{
            System.out.println("SAMPLE: Partition " + context.getPartitionId() + " is closing for reason " + reason.toString());
        }
    	
    	@Override
    	public void onError(PartitionContext context, Throwable error){
    		System.out.println("SAMPLE: Partition " + context.getPartitionId() + " onError: " + error.toString());
    	}

    	
    	@Override
        public void onEvents(PartitionContext context, Iterable<EventData> events) throws Exception{
    		
            System.out.println("SAMPLE: Partition " + context.getPartitionId() + " got event batch");
            int eventCount = 0;
            for (EventData data : events){
            	try{
            		/* sample output 
            		 *  SAMPLE (1,416,1): {"id":"3ac","strProperty":"Hello Event Hub","longProperty":0,"intProperty":0}
						SAMPLE: Partition 1 batch size was 1 for host MSSHostNamePrefix-c2abce28-651b-4926-9ec5-0582f47b8b73
						SAMPLE: Partition 0 batch size was 3 for host MSSHostNamePrefix-c2abce28-651b-4926-9ec5-0582f47b8b73
            		 */
	                System.out.println("SAMPLE (" + context.getPartitionId() + "," + data.getSystemProperties().getOffset() + "," +data.getSystemProperties().getSequenceNumber() + "): " + new String(data.getBytes(), "UTF8"));
	                eventCount++;
	                this.checkpointBatchingCount++;
	                if ((checkpointBatchingCount % 5) == 0){
	                	System.out.println("SAMPLE: Partition " + context.getPartitionId() + " checkpointing at " + data.getSystemProperties().getOffset() + "," + data.getSystemProperties().getSequenceNumber());
	                	context.checkpoint(data).get();
	                }
            	}catch (Exception e){
            		System.out.println("Processing failed for an event: " + e.toString());
            	}
            }
            System.out.println("SAMPLE: Partition " + context.getPartitionId() + " batch size was " + eventCount + " for host " + context.getOwner());
        }
    }
}
