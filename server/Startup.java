package simpledb.server;

import static simpledb.tx.recovery.LogRecord.CHECKPOINT;
import static simpledb.tx.recovery.LogRecord.COMMIT;
import static simpledb.tx.recovery.LogRecord.ROLLBACK;
import static simpledb.tx.recovery.LogRecord.SETINT;
import static simpledb.tx.recovery.LogRecord.SETSTRING;
import static simpledb.tx.recovery.LogRecord.START;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Iterator;

import simpledb.file.Block;
import simpledb.log.BasicLogSinglePageRecord;
import simpledb.remote.RemoteDriver;
import simpledb.remote.RemoteDriverImpl;
import simpledb.tx.Transaction;
/**
* Adding test cses. UUncomment and run each one to get the required results.
* @author Team number xxx
*/
public class Startup {
	public static void main(String args[]) throws Exception {
		// configure and initialize the database
		SimpleDB.init(args[0]);

		// create a registry specific for the server on the default port
		Registry reg = LocateRegistry.createRegistry(1099);

		// and post the server entry in it
		RemoteDriver d = new RemoteDriverImpl();
		reg.rebind("simpledb", d);
		//test1();//5
		//test2();//exception
		//test3();//
	//test4();//7
	//	test5();//no one
	//	test6(); //depends on the initial situation
		System.out.println("database server ready");
	}

	private static void test3() {
		// TODO Auto-generated method stub
		Block blk1 = new Block("filename", 1);
		Integer i1 = new Integer(1);
		Integer i2 = new Integer(2);
		String x = "AmIThere";
		Transaction tx = new Transaction();
		tx.pin(blk1);
		tx.setInt(blk1, 5, i1);
		tx.setInt(blk1, 10, i2);
		tx.setString(blk1, 17, x);
		printLogPageBuffer();

	}

	private static void printLogPageBuffer() {
		// TODO Auto-generated method stub
		System.out.println("Buffer Number pinned to the log block: " + SimpleDB.bufferMgr().getLoggingBuffer().getbNumber());
		System.out.println("Contents of the Buffer: "+ SimpleDB.bufferMgr().getLoggingBuffer().getbNumber());
		Iterator<BasicLogSinglePageRecord> iter = SimpleDB.logMgr().pageIterator();
		String x = "";
		while (iter.hasNext()) {
			BasicLogSinglePageRecord rec = iter.next();
			int op = rec.nextInt();
			switch (op) {
			case CHECKPOINT:
				x = "<CHECKPOINT>" + " " + x;
				break;
			case START:
				x = "<START " + rec.nextInt() + ">" + " " + x;
				break;
			case COMMIT:
				x = "<COMMIT " + rec.nextInt() + ">" + " " + " " + x;
				break;
			case ROLLBACK:
				x = "<ROLLBACK " + rec.nextInt() + ">" + " " + x;
				break;
			case SETINT:
				x = "<SETINT " + rec.nextInt() + " "
						+ new Block(rec.nextString(), rec.nextInt()) + " "
						+ rec.nextInt() + " " + rec.nextInt() + ">" + " " + x;
				break;
			case SETSTRING:
				x = "<SETSTRING " + rec.nextInt() + " "
						+ new Block(rec.nextString(), rec.nextInt()) + " "
						+ rec.nextInt() + " " + rec.nextString() + ">" + " "
						+ x;
				break;
			default:

			}
		}
		System.out.println(x);
	}

	private static void test2() {
		// TODO Auto-generated method stub
		Block blk1 = new Block("filename", 1);
		Block blk2 = new Block("filename", 2);
		Block blk3 = new Block("filename", 3);
		Block blk4 = new Block("filename", 4);
		Block blk5 = new Block("filename", 5);
		Block blk6 = new Block("filename", 6);
		Block blk7 = new Block("filename", 7);
		Block blk8 = new Block("filename", 8);
		Block blk9 = new Block("filename", 9);
		Transaction tx = new Transaction();
		tx.pin(blk1);
		tx.pin(blk2);
		tx.pin(blk3);
		tx.pin(blk4);
		tx.pin(blk5);
		tx.pin(blk6);
		tx.pin(blk7);
		tx.pin(blk8);
		tx.pin(blk9);

	}

	private static void test1() {
		Block blk1 = new Block("filename", 1);
		Block blk2 = new Block("filename", 2);
		Block blk3 = new Block("filename", 3);
		Block blk4 = new Block("filename", 4);
		Block blk5 = new Block("filename", 5);
		Block blk6 = new Block("filename", 6);
		Block blk7 = new Block("filename", 7);
		Block blk8 = new Block("filename", 8);
		Block blk9 = new Block("filename", 9);
		Transaction tx = new Transaction();
		tx.pin(blk1);
		tx.pin(blk2);
		tx.pin(blk3);
		tx.pin(blk4);
		tx.pin(blk5);
		tx.pin(blk6);
		tx.pin(blk7);
		tx.pin(blk8);
		tx.pin(blk4);
		tx.pin(blk2);
		tx.pin(blk7);
		tx.pin(blk1);
		tx.unpin(blk8);
		tx.unpin(blk7);
		tx.unpin(blk6);
		tx.unpin(blk5);
		tx.unpin(blk4);
		tx.unpin(blk1);
		tx.unpin(blk7);
		tx.unpin(blk4);
		tx.unpin(blk2);
		tx.unpin(blk2);
		ArrayList<Integer> listInit = SimpleDB.bufferMgr().getListOfBlocksInBuffers();
		tx.pin(blk9);
		ArrayList<Integer> listFinal = SimpleDB.bufferMgr().getListOfBlocksInBuffers();
		System.out.println("The buffer chosen for replacement is: " + diffrnt(listInit,listFinal));

	}
	
	private static void test4() {
		Block blk1 = new Block("filename", 1);
		Block blk2 = new Block("filename", 2);
		Block blk3 = new Block("filename", 3);
		Block blk4 = new Block("filename", 4);
		Block blk5 = new Block("filename", 5);
		Block blk6 = new Block("filename", 6);
		Block blk7 = new Block("filename", 7);
		Block blk8 = new Block("filename", 8);
		Block blk9 = new Block("filename", 9);
		Transaction tx = new Transaction();
		tx.pin(blk1);
		tx.pin(blk2);
		tx.pin(blk3);
		tx.pin(blk4);
		tx.pin(blk5);
		tx.pin(blk6);
		tx.pin(blk7);
		tx.pin(blk8);
		tx.pin(blk8);
		tx.pin(blk7);
		tx.pin(blk6);
		tx.pin(blk5);
		tx.pin(blk4);
		tx.pin(blk3);
		tx.pin(blk2);
		tx.pin(blk1);
		tx.unpin(blk8);
		tx.unpin(blk8);
		tx.unpin(blk7);
		tx.unpin(blk7);
		ArrayList<Integer> listInit = SimpleDB.bufferMgr().getListOfBlocksInBuffers();
		tx.pin(blk9);
		ArrayList<Integer> listFinal = SimpleDB.bufferMgr().getListOfBlocksInBuffers();
		System.out.println("The buffer chosen for replacement is: " + diffrnt(listInit,listFinal));

	}
	
	private static void test5() {
		// TODO Auto-generated method stub
		Block blk1 = new Block("filename", 1);
		Block blk2 = new Block("filename", 2);
		Block blk3 = new Block("filename", 3);
		Block blk4 = new Block("filename", 4);
		Block blk5 = new Block("filename", 5);
		Block blk6 = new Block("filename", 6);
		Block blk7 = new Block("filename", 7);
		Block blk8 = new Block("filename", 8);
		Transaction tx = new Transaction();
		tx.pin(blk1);
		tx.pin(blk2);
		tx.pin(blk3);
		tx.pin(blk4);
		tx.pin(blk5);
		tx.pin(blk6);
		tx.pin(blk7);
		tx.pin(blk8);
		ArrayList<Integer> listInit = SimpleDB.bufferMgr().getListOfBlocksInBuffers();
		tx.pin(blk8);
		ArrayList<Integer> listFinal = SimpleDB.bufferMgr().getListOfBlocksInBuffers();
		System.out.println("The buffer chosen for replacement is: " + diffrnt(listInit,listFinal));

	}
	
	private static void test6() {
		// TODO Auto-generated method stub
		Block blk1 = new Block("filename", 1);
		Block blk2 = new Block("filename", 2);
		Block blk3 = new Block("filename", 3);
		Transaction tx = new Transaction();
		tx.pin(blk1);
		tx.pin(blk2);
		ArrayList<Integer> listInit = SimpleDB.bufferMgr().getListOfBlocksInBuffers();
		tx.pin(blk3);
		ArrayList<Integer> listFinal = SimpleDB.bufferMgr().getListOfBlocksInBuffers();
		System.out.println("The buffer chosen for replacement is: " + diffrnt(listInit,listFinal));
		
	}


	private static String diffrnt(ArrayList<Integer> listInit, ArrayList<Integer> listFinal) {
		// TODO Auto-generated method stub
		if(listInit.size() != listFinal.size()){
			return "Buffer with empty Block replaced";
		}
		for(int i=0; i <listInit.size();i++){
			if(listInit.get(i) != listFinal.get(i)){
				return ""+listInit.get(i);
			}
		}
		
		return "No one";
	}

}
