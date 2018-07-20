import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class Controller 
{
	private static int totalMoneyInBank;
	private int moneyForEachBranch;
	private HashMap<String, Socket> sockets;

	public Controller() 
	{
		sockets = new HashMap<String, Socket>();
	}

	public static void main(String[] args) 
	{
		if (args.length != 2)
		{
			System.err.println("Usage: Controller Total_Money_In_Bank BRANCHES_FILE");
			System.exit(-1);
		}

		// Read the existing branches list.
		try 
		{
			totalMoneyInBank = Integer.parseInt(args[0]);
			FileReader input = new FileReader(new File(args[1]));

			BufferedReader buffReader = new BufferedReader(input);

			Controller c = new Controller();
			c.operations(buffReader);

		}
		catch(NumberFormatException nfe)
		{
			System.err.println("Invalid input " + args[0] + " for Total_Money_In_Bank, should be an integer");
			System.exit(-1);
		}
		catch (FileNotFoundException e) 
		{
			System.out.println(args[1] + ": File not found.");
			System.exit(-1);
		}
	}

	public void operations(BufferedReader buffReader)
	{
		String line = "";
		String temp[];
		try
		{
			int i = 0;
			Bank.InitBranch.Builder ib = Bank.InitBranch.newBuilder();
			while((line = buffReader.readLine()) != null)
			{
				temp = line.split("\\s+");
				if(temp.length != 3)
				{
					System.err.println("Input file content not in proper format");
					System.exit(-1);
				}
				Bank.InitBranch.Branch branch = Bank.InitBranch.Branch.newBuilder().setName(temp[0]).setIp(temp[1]).setPort(Integer.parseInt(temp[2])).build();
				ib.addAllBranches(i, branch);
				i++;
			}

			moneyForEachBranch = totalMoneyInBank / ib.getAllBranchesCount();
			ib.setBalance(moneyForEachBranch);

			Bank.BranchMessage.Builder messageBuilder = Bank.BranchMessage.newBuilder();
			messageBuilder.setInitBranch(ib);

			for(int j = 0; j < ib.getAllBranchesCount(); j++)
			{
				//Sending InitBranch Message
				Socket client = new Socket(ib.getAllBranches(j).getIp(), ib.getAllBranches(j).getPort());
				OutputStream objectToServer = client.getOutputStream();
				DataOutputStream sourceStream = new DataOutputStream(client.getOutputStream());

				sockets.put(ib.getAllBranches(j).getName(), client);
				sourceStream.writeUTF("Controller");
				messageBuilder.build().writeDelimitedTo(objectToServer);
			}

			int snapshot_id = 1;
			while(true)
			{


				Thread.sleep(2000);

				//SnapShot Start
				String randomBranch = getRandomBranch(ib.getAllBranchesList());
				Bank.InitSnapshot.Builder initSnapshotBuilder = Bank.InitSnapshot.newBuilder();
				initSnapshotBuilder.setSnapshotId(snapshot_id);

				messageBuilder.clearBranchMessage();
				messageBuilder.setInitSnapshot(initSnapshotBuilder);

				//Sending InitSnapShot Message
				sendMessage(randomBranch, messageBuilder.build());

				Thread.sleep(10000);

				//Retrieve Snapshot from all branches
				Bank.RetrieveSnapshot.Builder retSnapBuilder = Bank.RetrieveSnapshot.newBuilder();
				retSnapBuilder.setSnapshotId(snapshot_id);

				messageBuilder.clearBranchMessage();
				messageBuilder.setRetrieveSnapshot(retSnapBuilder);

				System.out.println("snapshot_id: " + snapshot_id);
				for (int no = 0; no < ib.getAllBranchesCount(); no++)
				{
					sendAndReceiveMessage(ib.getAllBranches(no).getName(), messageBuilder.build());
				}
				System.out.println();

				snapshot_id++;
			}
		}
		catch (InterruptedException ie)
		{
			ie.printStackTrace();
		}
		catch (IOException e) 
		{
			e.printStackTrace();
		} 

		finally 
		{
			try
			{ 
				buffReader.close(); 
			} 
			catch (Throwable ignore) 
			{}
		}
	}

	private void sendMessage(String someBranch, Bank.BranchMessage message) throws IOException
	{
		Socket client = sockets.get(someBranch);
		OutputStream messageStreamToServer = client.getOutputStream();
		message.writeDelimitedTo(messageStreamToServer);
	}

	private void sendAndReceiveMessage(String someBranch, Bank.BranchMessage message) throws IOException
	{
		Socket client = sockets.get(someBranch);

		OutputStream messageStreamToServer = client.getOutputStream();
		InputStream messageStreamFromServer = client.getInputStream();
		message.writeDelimitedTo(messageStreamToServer);

		Bank.BranchMessage messFromServ = Bank.BranchMessage.parseDelimitedFrom(messageStreamFromServer);
		StringBuffer sb = new StringBuffer();
		sb.append(someBranch+": " + messFromServ.getReturnSnapshot().getLocalSnapshot().getBalance() + ", ");
		for(int i = 0; i < messFromServ.getReturnSnapshot().getLocalSnapshot().getChannelStateCount(); i++)
		{
			if(messFromServ.getReturnSnapshot().getLocalSnapshot().getChannelStateList().get(i) < 0)
			{
				if(i>0)
				{
					sb.append(", ");
				}
				int sourceNo = messFromServ.getReturnSnapshot().getLocalSnapshot().getChannelStateList().get(i);
				sourceNo = -1*sourceNo;
				String source = "branch"+sourceNo + "->" + someBranch + ": ";
				sb.append(source);
			}
			else
				sb.append(messFromServ.getReturnSnapshot().getLocalSnapshot().getChannelStateList().get(i) + " ");
		}
		System.out.println(sb.toString());
	}

	private static String getRandomBranch(List<Bank.InitBranch.Branch> allBranches)
	{
		Bank.InitBranch.Branch randomBranch = null;

		Random randInt = new Random();
		int index = (int) (randInt.nextFloat() * allBranches.size());
		randomBranch = allBranches.get(index);

		return randomBranch.getName();
	}
}