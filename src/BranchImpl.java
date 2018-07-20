import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class BranchImpl
{
	static String branchName;
	
	public BranchImpl() 
	{
	}

	public static void main(String[] args) 
	{
		if (args.length != 2)
		{
			System.err.println("Usage: Branch Branch_Name Port_Number_Branch_Running_On");
			System.exit(-1);
		}

		try
		{
			String ipAddress = InetAddress.getLocalHost().getHostAddress();
			branchName = args[0];
			int portNumber = Integer.parseInt(args[1]);
			BranchImpl b = new BranchImpl();
			b.createSocket(branchName, ipAddress, portNumber);
		}
		catch(NumberFormatException nfe)
		{
			System.err.println("Port_Number_Branch_Running_On Should be an Integer");
			System.exit(-1);
		} 
		catch (UnknownHostException e) 
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public void createSocket(String name, String ip, int portNo)
	{
		ServerSocket server;
		try 
		{
			server = new ServerSocket(portNo);
			System.out.println("Server started on:");
			System.out.println("Host Address: " + ip);
			System.out.println("Port Number: " + portNo);
			System.out.println();
			
			BranchReceiver serverRunnable = BranchReceiver.getInstance(branchName);
			while(true)
			{
				Socket clientSocket = server.accept();
				
				String source = "";

				DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
				source = dis.readUTF();

				if(!serverRunnable.socketConnections.containsKey(source))
				{
					serverRunnable.socketConnections.put(source, clientSocket);
				}
				
				String connectedTo = "";
				for (String s : serverRunnable.socketConnections.keySet())
				{
					if(serverRunnable.socketConnections.get(s).equals(clientSocket))
					{
						connectedTo = s;
						break;
					}
				}

				serverRunnable.setConnectionBranch(connectedTo);
				Thread serverThread = new Thread(serverRunnable, name);
				serverThread.start();
				Thread.sleep(300);
			}
		}
		catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
		catch (IOException e) 
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
