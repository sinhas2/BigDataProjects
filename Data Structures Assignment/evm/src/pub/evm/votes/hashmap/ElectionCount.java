package pub.evm.votes.hashmap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

class Vote {    
	int voterId; 
	int candidateId; 
}

/**
 * ElectionCount class using HashMap
 * @author Shubhra Sinha
 *
 */
class ElectionCount{
	Hashtable<Integer, ArrayList<Vote>> voteList;
	
	/**
	 * Constructor
	 */
	ElectionCount() {
		//create new hashmap
		voteList = new Hashtable<Integer, ArrayList<Vote>>();
	}
	
	/**
	 * Adds the vote into Data Structure
	 * @param voterId
	 * @param candidateId
	 */
	public void add(int voterId, int candidateId) {
		//Create new vote using voterId and candidateId
		//Add the new Vote using voterId as key
		//Also add the new Vote using candidateId as key
		Vote vote = new Vote();
		vote.voterId = voterId;
		vote.candidateId = candidateId;
		if(voteList.containsKey(voterId)) {
			voteList.get(voterId).add(vote);
		} else {
			ArrayList<Vote> ar = new ArrayList<Vote>();
			ar.add(vote);
			voteList.put(voterId, ar);
		}
		if(voteList.containsKey(candidateId)) {
			voteList.get(candidateId).add(vote);
		} else {
			ArrayList<Vote> ar = new ArrayList<Vote>();
			ar.add(vote);
			voteList.put(candidateId, ar);
		}
		System.out.println("Successfully added voterId " + voterId + " with candidateId " + candidateId);
	}
	
	/**
	 * Finds the candidateId for a given voterId from List
	 * If voterId is not found then return zero
	 * @param voterId
	 * @return
	 */
	public int find(int voterId) {
		//check if map contains the given voterId
		//if found then return the candidateId or else return 0
		if(voteList.containsKey(voterId)) {
			return voteList.get(voterId).get(0).candidateId;
		} else {
			return 0;
		}
	}

	/**
	 * Counts total number of votes given by a candidate
	 * If candidateId is not found then return zero
	 * @param candidateId
	 * @return
	 */
	public int count(int candidateId) {
		//Find if map contains the given candidateId 
		//If yes then return number of values it contains else 0
		if (voteList.containsKey(candidateId))
			return voteList.get(candidateId).size();
		else
			return 0;
	}
	
	/**
	 * MAIN
	 * @param args
	 */
	public static void main(String[] args) {

		//Creating new instance of class for methods call
		ElectionCount ec = new ElectionCount();
		
		try {
			//Read the input from file line by line and store in list
			File file = new File("C:\\Users\\prabitha\\Desktop\\data.txt");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			StringBuffer stringBuffer = new StringBuffer();
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String[] temp = line.split("	");
				int voterId = Integer.valueOf(temp[0].trim());
				int candidateId = Integer.valueOf(temp[1].trim());
				//System.out.println("voterId: " + voterId);
				//System.out.println("candidateId: " + candidateId);
				ec.add(voterId,candidateId);
			}
			//close the file
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//find example
		if (ec.find(100000) != 0)
			System.out.println("VoteId 100000 draws vote for " + ec.find(100000));
		
		//count example
		System.out.println("Total number of votes received for candidateId 135 are : " + ec.count(135));
		
		
	}

} 
