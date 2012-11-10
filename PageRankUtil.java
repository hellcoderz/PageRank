public class PageRankUtil{	//util class to hold counters
	public static long no_of_nodes;	//hold number of nodes
	public static long no_of_edges;	//hold number of edges
	public static long min_outlinks;	//minimum number of outlinks
	public static long max_outlinks;	//maximum number of outlinks

	PageRankUtil(){
		no_of_nodes = 0;
		no_of_edges = 0;
		min_outlinks = 99999999;
		max_outlinks = 0;
	}
}