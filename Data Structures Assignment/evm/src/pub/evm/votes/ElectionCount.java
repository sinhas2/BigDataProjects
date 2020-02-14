package pub.evm.votes;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

class Vote {    
	int voterId; 
	int candidateId; 
}

/**
 * ElectionCount class for Direct Addressing
 * @author Shubhra Sinha
 *
 */
class ElectionCount{
	Vote[] voteList;
	int[] candidateList;
	
	/**
	 * Constructor
	 */
	ElectionCount() {
		//Declares list to store vote details for 6-digit long voter IDs.
		voteList = new Vote[900000];
		
		//Declares list to store candidate vote count details for 3-digit long candidate IDs.
		candidateList = new int[900];
	}
	
	/**
	 * Adds the vote into Data Structure
	 * @param voterId
	 * @param candidateId
	 */
	public void add(int voterId, int candidateId) {
		//Create new vote using voterId and candidateId
		//Calculate the index using voterId for direct addressing
		//Add the new vote at calculated index in list
		Vote vote = new Vote();
		vote.voterId = voterId;
		vote.candidateId = candidateId;
		voteList[voterId-100000] = vote;
		
		//Calculate the index using candidateId for direct addressing
		//Increase the count for each received vote
		if (candidateList[candidateId-100] == 0)
			candidateList[candidateId-100] = 1;
		else 
			candidateList[candidateId-100] = candidateList[candidateId-100] + 1;
		
		System.out.println("Successfully added voterId " + voterId + " with candidateId " + candidateId);
	}
	
	/**
	 * Finds the candidateId for a given voterId from List
	 * @param voterId
	 * @return
	 */
	public int find(int voterId) {
		//Calculate index using voterId and check if value is not null at calculated index
		//and check in the calculated index voter ID is same as given voter ID
		//if found then return the candidateId or else return 0
		if(voteList[voterId-100000] != null && voteList[voterId-100000].voterId == voterId) {
			return voteList[voterId-100000].candidateId;
		} else {
			return 0;
		}
	}

	/**
	 * Counts total number of votes given by a candidate
	 * @param candidateId
	 * @return
	 */
	public int count(int candidateId) {
		//get the count for the provided candidateId
		return candidateList[candidateId-100];
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