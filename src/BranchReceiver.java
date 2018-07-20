import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

//Server
public class BranchReceiver implements Runnable 
{
	private int branchBalance;
	private int initialBranchBalance;
	private String branchName;
	private ArrayList<Bank.InitBranch.Branch> allBranches;
	private volatile static BranchReceiver receiverUniqueInstance;
	private int markersReceived;

	private HashMap<Integer, Integer> localState = new HashMap<Integer, Integer>();
	private HashMap<String, ArrayList<Integer>> incomingChannel = new HashMap<String, ArrayList<Integer>>();
	private HashMap<String, Boolean> snapshotState = new HashMap<String, Boolean>();


	private boolean canTransfer = false;
	private boolean flag = false;	
	private HashMap<String, Boolean> markersSentTo = new HashMap<String, Boolean>();

	HashMap<String, Socket> socketConnections;

	private BranchReceiver() 
	{
		allBranches = new ArrayList<Bank.InitBranch.Branch>();
		socketConnections = new HashMap<String, Socket>();
	}

	private BranchReceiver(String bName)
	{
		this();
		branchName = bName;
	}

	public static BranchReceiver getInstance(String bName)
	{
		if(receiverUniqueInstance == null)
		{
			synchronized (BranchReceiver.class) 
			{
				if(receiverUniqueInstance == null)
				{
					receiverUniqueInstance = new BranchReceiver(bName);
				}
			}
		}
		return receiverUniqueInstance;
	}

	String connectionBranchName;

	public void setConnectionBranch(String branchName)
	{
		connectionBranchName = branchName;
	}

	@Override
	public void run() 
	{
		try
		{
			Socket s = socketConnections.get(connectionBranchName);
			String connectedTo = connectionBranchName;

			InputStream incomingStream = s.getInputStream();

			Bank.BranchMessage msg = null;
			while((msg = Bank.BranchMessage.parseDelimitedFrom(incomingStream)) != null)
			{
				synchronized (BranchReceiver.class) 
				{
					switch(msg.getBranchMessageCase().getNumber())
					{
					case 0:
						break;
					case 1: 
						initBranch(msg.getInitBranch().getBalance(), msg.getInitBranch().getAllBranchesList());
						break;
					case 2:
						transfer(msg, connectedTo);
						break;
					case 3:
						initSnapshot(msg);
						break;
					case 4:
						marker(msg, connectedTo);
						break;
					case 5:
						retrieveSnapshot(msg, connectedTo);
						break;
					case 6:
						break;
					default:
						System.out.println("No Such Message Handle Found");
					}
					msg = null;
				}
			}
		}
		catch (IOException e) 
		{
			System.err.println();
			e.printStackTrace();
			System.exit(-1);
		}
	}



	public void initBranch(int balance, List<Bank.InitBranch.Branch> list)
	{
		//System.out.println("Init Branch Message Received");

		branchBalance = balance;
		initialBranchBalance = balance;

		for (Bank.InitBranch.Branch b : list)
		{
			allBranches.add(b);
		}
		canTransfer = true;

		for(int currentBranchIndex = allBranches.size()-1; currentBranchIndex >= 0; currentBranchIndex--)
		{
			if(allBranches.get(currentBranchIndex).getName().equals(branchName))
			{
				for(int index = currentBranchIndex-1; index >= 0 ; index--)
				{
					try 
					{
						Socket s = new Socket(allBranches.get(index).getIp(), allBranches.get(index).getPort());
						socketConnections.put(allBranches.get(index).getName(), s);
						DataOutputStream dos = new DataOutputStream(s.getOutputStream());
						dos.writeUTF(branchName);
						dos.flush();

						BranchReceiver serverRunnable = BranchReceiver.getInstance(branchName);
						serverRunnable.setConnectionBranch(allBranches.get(index).getName());
						Thread serverThread = new Thread(serverRunnable, branchName);
						serverThread.start();
						Thread.sleep(300);
					}
					catch (InterruptedException e) 
					{
						e.printStackTrace();
					}
					catch (UnknownHostException e)
					{
						e.printStackTrace();
					}
					catch (IOException e) 
					{
						e.printStackTrace();
					}
				}
			}
		}

		try 
		{
			Thread.sleep(3000);
		}
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}

		new Thread(new Runnable() 
		{
			@Override
			public void run() 
			{
				scheduleTransfer();
			}
		}).start();
	}

	public void transfer(Bank.BranchMessage transferMessage, String source)
	{
		if(!snapshotState.isEmpty())
		{
			for(int x = 1; x <= snapshotState.size(); x++)
			{
				if(snapshotState.containsKey(x+":"+source) && snapshotState.get(x+":"+source))
				{
					addValuesToIncomingChannel(x+":"+source, transferMessage.getTransfer().getMoney());
				}
			}
		}

		//System.out.println("Received : " + transferMessage.getTransfer().getMoney());
		branchBalance += transferMessage.getTransfer().getMoney();
		//System.out.println(branchName + " : " + branchBalance + " after");
	}

	private void addValuesToIncomingChannel(String key, int value)
	{
		ArrayList<Integer> tempList = null;
		if (incomingChannel.containsKey(key)) 
		{
			tempList = incomingChannel.get(key);
			if(tempList == null)
				tempList = new ArrayList<Integer>();
			tempList.add(value);
		} 
		else 
		{
			tempList = new ArrayList<Integer>();
			tempList.add(value);   
		}
		incomingChannel.put(key, tempList);
	}

	private void scheduleTransfer()
	{
		Timer timer = new Timer("TransferThread");

		class ScheduleTask extends TimerTask
		{
			@Override
			public void run() 
			{
				String target = getRandomBranch();
				int amount = getAmountToTransfer();
				//System.out.println(branchName + " : sending to "+target+", Amount : "+amount );

				//synchronized (BranchReceiver.class)
				{
					if(amount != -1)
					{
						//System.out.println(branchName + " : " + branchBalance + " beforeinsync");
						int test = branchBalance - amount;
						if(test >= 0)
						{
							branchBalance = test;
							//System.out.println(amount);
							//System.out.println(branchName + " : " + branchBalance + " afterinsync");

							Bank.Transfer.Builder transferBuilder = Bank.Transfer.newBuilder();
							transferBuilder.setMoney(amount);

							Bank.BranchMessage.Builder transferMessageBuilder = Bank.BranchMessage.newBuilder();
							transferMessageBuilder.setTransfer(transferBuilder);

							Socket client = socketConnections.get(target);
							try 
							{
								transferMessageBuilder.build().writeDelimitedTo(client.getOutputStream());
							} 
							catch (IOException e) 
							{
								e.printStackTrace();
							}

							if(!canTransfer)
							{
								timer.cancel();
							}
							else
							{
								while(flag)
								{

								}
								//timer.schedule(new ScheduleTask(), 3000);
								timer.schedule(new ScheduleTask(), 1000*getRandomTimeInterval());
							}
						}
					}
				}
			}
		};
		new ScheduleTask().run();
	}

	public void connectServer(Bank.BranchMessage message, String target)
	{
		Socket client = socketConnections.get(target);
		{
			try 
			{
				OutputStream returnStream = new DataOutputStream(client.getOutputStream());
				message.writeDelimitedTo(returnStream);
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
	}

	private int getAmountToTransfer() 
	{
		if(0 == branchBalance)
		{
			canTransfer = false;
			return -1;
		}
		else
		{
			Random r = new Random();
			return (int) (r.nextInt((int) ((0.05*initialBranchBalance - 0.01*initialBranchBalance)+1)) + 0.01*initialBranchBalance);
		}
	}


	public void initSnapshot(Bank.BranchMessage initSnapshotMessage)
	{
		//System.out.println("Init Snapshot");
		localState.put(initSnapshotMessage.getInitSnapshot().getSnapshotId(), branchBalance);
		setFlag(true);
		markersReceived++;

		Bank.Marker.Builder markerBuilder = Bank.Marker.newBuilder();
		markerBuilder.setSnapshotId(initSnapshotMessage.getInitSnapshot().getSnapshotId());

		Bank.BranchMessage.Builder markerMessageBuilder = Bank.BranchMessage.newBuilder();
		markerMessageBuilder.setMarker(markerBuilder);

		for(int x = 0; x < allBranches.size(); x++)
		{
			if(!allBranches.get(x).getName().equals(branchName))
			{
				snapshotState.put(initSnapshotMessage.getInitSnapshot().getSnapshotId()+":"+allBranches.get(x).getName(), true);
				incomingChannel.put(initSnapshotMessage.getInitSnapshot().getSnapshotId()+":"+allBranches.get(x).getName(), new ArrayList<Integer>());
				//System.out.println("Marker Sent to" + allBranches.get(x).getName());
				connectServer(markerMessageBuilder.build(), allBranches.get(x).getName());

			}
		}
		setFlag(false);
	}


	public void marker(Bank.BranchMessage markerMessage, String source)
	{
		//System.out.println("Marker from " + source);
		setFlag(true);
		markersReceived++;

		if(markersReceived == 1)
		{
			localState.put(markerMessage.getMarker().getSnapshotId(), branchBalance);
			//Mark Incoming Channel as empty
			markIncomingChannelEmpty(markerMessage.getMarker().getSnapshotId()+":"+source);
			snapshotState.put(markerMessage.getMarker().getSnapshotId()+":"+source, false);

			/*			while(markersSentTo.size() != 3)
			{
				String markerToSendToBranch = getRandomBranch();
				while(markersSentTo.containsKey(markerToSendToBranch) && markersSentTo.get(markerToSendToBranch))
				{
					markerToSendToBranch = getRandomBranch();
				}

				markersSentTo.put(markerToSendToBranch, true);

				if(!markerToSendToBranch.equals(source))
				{
					//Key = snapshotId:<target> Value = false
					snapshotState.put(markerMessage.getMarker().getSnapshotId() + ":" + markerToSendToBranch, true);
					incomingChannel.put(markerMessage.getMarker().getSnapshotId() + ":" + markerToSendToBranch, new ArrayList<Integer>());
				}
				System.out.println("Marker Sent to" + markerToSendToBranch);
				connectServer(markerMessage, markerToSendToBranch);
			}*/
			for(int x = 0; x < allBranches.size(); x++)
			{
				if(!allBranches.get(x).getName().equals(branchName))
				{
					if(!allBranches.get(x).getName().equals(source))
					{
						//Key = snapshotId:<target> Value = false
						snapshotState.put(markerMessage.getMarker().getSnapshotId() + ":" + allBranches.get(x).getName(), true);
						incomingChannel.put(markerMessage.getMarker().getSnapshotId() + ":" + allBranches.get(x).getName(), new ArrayList<Integer>());
					}
					//System.out.println("Marker Sent to" + allBranches.get(x).getName());
					connectServer(markerMessage, allBranches.get(x).getName());
				}
			}
		}
		if(markersReceived > 1)
		{
			//Key = snapshotId:<source> Value = false
			snapshotState.replace(markerMessage.getMarker().getSnapshotId()+":"+source, false);
		}
		setFlag(false);
	}


	private void markIncomingChannelEmpty(String key)
	{
		ArrayList<Integer> tempList = null;
		if (incomingChannel.containsKey(key))
		{
			tempList = incomingChannel.get(key);
			if(tempList == null)
				tempList = new ArrayList<Integer>();
			tempList.clear();
		}
		else
		{
			tempList = new ArrayList<Integer>();
			tempList.clear();
		}
		incomingChannel.put(key, tempList);
	}

	public void retrieveSnapshot(Bank.BranchMessage retrieveSnapshotMessage, String source) throws IOException
	{
		//System.out.println("Retrieve Snapshot");
		Bank.ReturnSnapshot.LocalSnapshot.Builder localSnapshotBuilder = Bank.ReturnSnapshot.LocalSnapshot.newBuilder();
		try
		{
			localSnapshotBuilder.setBalance(localState.get(retrieveSnapshotMessage.getRetrieveSnapshot().getSnapshotId()));
			localSnapshotBuilder.setSnapshotId(retrieveSnapshotMessage.getRetrieveSnapshot().getSnapshotId());
			List<Integer> channelState = new ArrayList<Integer>();

			int x = 0;
			for (String s : incomingChannel.keySet())
			{
				String temp[] = s.split(":");
				if(retrieveSnapshotMessage.getRetrieveSnapshot().getSnapshotId() == Integer.parseInt(temp[0]))
				{
					channelState.add(x, Integer.parseInt(""+temp[1].charAt(6))*-1);
					x++;
					ArrayList<Integer> amountList = incomingChannel.get(s);
					if(!amountList.isEmpty())
					{
						for(int a = 0; a < amountList.size(); a++)
						{
							channelState.add(x+a, amountList.get(a));
						}
						x += amountList.size();
					}
					else
					{
						channelState.add(x, 0);
						x += 1;
					}


				}
			}

			markersSentTo.clear();
			markersReceived = 0;
			setFlag(false);


			localSnapshotBuilder.addAllChannelState(channelState);

			Bank.ReturnSnapshot.Builder returnSnapshotBuilder = Bank.ReturnSnapshot.newBuilder();
			returnSnapshotBuilder.setLocalSnapshot(localSnapshotBuilder);

			Bank.BranchMessage.Builder messageBuilder = Bank.BranchMessage.newBuilder();
			messageBuilder.setReturnSnapshot(returnSnapshotBuilder);

			connectServer(messageBuilder.build(), source);
		}
		catch(NullPointerException npe)
		{
			npe.printStackTrace();
			System.exit(-1);
		}
	}

	private int getRandomTimeInterval() 
	{
		Random r = new Random();
		return r.nextInt(6); //returns a random interval between 0-5
	}

	//private synchronized String getRandomBranch()
	private String getRandomBranch()
	{
		Bank.InitBranch.Branch randomBranch = null;
		do
		{
			Random randInt = new Random();
			int index = (int) (randInt.nextFloat() * allBranches.size());
			randomBranch = allBranches.get(index);
		}
		while(randomBranch.getName().equals(branchName));

		return randomBranch.getName();
	}

	private synchronized void setFlag(boolean value)
	{
		flag = value;
	}
}