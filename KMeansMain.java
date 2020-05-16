
public class KMeansMain {

	//1. Pick number of clusters->k
	//2. Make k points the centroid of a cluster
	//3. Randomize all the points location 
	
	public static void main(String[] args) {
		int data[]={2,4,-10,12,3,20,30,11}; // initial data
		int numberClusters=3;
		int centroid[][]=new int[][]{
			{0,0,0}, //old centroids
			{2,4,30} //new centroids->random
		};
		getCentroid(data,numberClusters,centroid);
	}
	
	public static int[][] getCentroid(int data[],int numberClusters,int centroid[][]){
		
		int distance[][]=new int[numberClusters][data.length];
		int cluster[]=new int[data.length];
		int clusternodecount[]=new int[numberClusters];
		
		centroid[0]=centroid[1]; //old centroid takes what it was the new ones
		centroid[1]=new int[]{0,0,0}; //basic assignment to new centroids
		System.out.println("========== Starting to get new centroid =========");
		
		for(int i=0;i<numberClusters;i++){ // 4. Calculate euclidean distance from each point to all the centroids
			for(int j=0;j<data.length;j++){
				distance[i][j]=Math.abs(data[j]-centroid[0][i]); //for each point,euclidean distance with the centroid of the i cluster
				System.out.print(distance[i][j]+" ,");
			}
			System.out.println();
		}
		
		for(int j=0;j<data.length;j++){ // 5. Assign membership of each point to the nearest centroid
			int smallerDistance=0;
			if(distance[0][j]<distance[1][j] && distance[0][j]<distance[2][j])
				smallerDistance=0; //so the point is closer to the centroid of cluster 0
			if(distance[1][j]<distance[0][j] && distance[1][j]<distance[2][j])
				smallerDistance=1; //the point is closer to the centroid of cluster 1
			if(distance[2][j]<distance[0][j] && distance[2][j]<distance[1][j])
				smallerDistance=2;// //the point is closer to the centroid of cluster 2
			
			centroid[1][smallerDistance]=centroid[1][smallerDistance]+data[j];  //sum of all the points in the cluster
			clusternodecount[smallerDistance]=clusternodecount[smallerDistance]+1; //increase number of node in the cluster->later needed to do the average and find new centroids
			cluster[j]=smallerDistance; //to which cluster belongs the point j right now
		}
		
        		System.out.println("======================================== ");
		
                System.out.println("New clusters are ");
              	 for(int i=0;i<numberClusters;i++){				
			        System.out.print("C"+(i+1)+": ");
                     for(int l=0;l<data.length;l++){
					if(cluster[l]==i)
						System.out.print(data[l]+" ,");
					
				}
				System.out.println();
			}
                System.out.println("======================================== ");
		//6. Establish the new centroids by averageing locations of all points belonging to a  given cluster	        
		System.out.println("New centroid is ");
		
		for(int j=0;j<numberClusters;j++){
			centroid[1][j]=centroid[1][j]/clusternodecount[j];
			System.out.print(centroid[1][j]+",");
		}
		System.out.println();
	
		//7. Stop Condition:No change in the centroids between the actual step and the previous one
		
		boolean isAchived=true;
		for(int j=0;j<numberClusters;j++){
			if(isAchived && centroid[0][j] == centroid[1][j]){
				isAchived=true;
				continue;
			}
			isAchived=false;
		}
		
		if(!isAchived){
                    
			getCentroid(data,numberClusters,centroid);
		}
		
		if(isAchived){
			System.out.println("======================================== ");
			System.out.println(" Final Cluster is ");
			for(int i=0;i<numberClusters;i++){	
                              System.out.print("C"+(i+1)+":");
				for(int j=0;j<data.length;j++){
					if(cluster[j]==i)
						System.out.print(data[j]+" ,");
					
				}
				System.out.println();
			}
		}
		
		return centroid;
		
	}

}
