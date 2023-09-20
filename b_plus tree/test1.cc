#include "./bpt.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
int search(const key_t& key, value_t *value) const;
int search_range(key_t *left, const key_t &right,
                     value_t *values, size_t max, bool *next = NULL) const;
int remove(const key_t& key);
int insert(const key_t& key, value_t value);
int update(const key_t& key, value_t value);


*/
using bpt::bplus_tree;
int main(){
	bplus_tree tree("test.db", true);
	tree.insert("11","aaaaaaaa");
	tree.insert("22","bbbbbbb");
	tree.insert("33","sssss");
	tree.insert("12","abcabc");
	tree.insert("13","dddddd");
	tree.insert("10","1111");
		
	
	bpt::value_t out;
	tree.search("11",&out);
	printf("%s\n",out.v);

	
	bpt::key_t key1("11"), key2("13");
	bpt::value_t *out1;
	out1=new bpt::value_t[100];
	tree.search_range(&key1,key2,out1,3);
	for(int i=0;i<3;++i){
		printf("%s\n",out1->v);
		out1++;
	}
	out1-=3;
	delete out1;
	
	tree.update("11","sb");
	tree.search("11",&out);
	printf("%s\n",out.v);


	bpt::key_t key_out;
	tree.search_left_key(&key_out);
       printf("%s\n",key_out.k);
	
	tree.search_right_key(&key_out);
       printf("%s\n",key_out.k);


	
	return 0;
		
}
