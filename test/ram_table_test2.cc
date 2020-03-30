#include <vector>
#include <iostream>
#include <algorithm>
#include <string>

using namespace std;

bool compare(string a, string b) {
    return a.compare(b) < 0 ;
}

int main() {

    std::vector<string> int_arr;
    int_arr.push_back("234");
    int_arr.push_back("256");

    std::vector<string>::const_iterator it = int_arr.begin();

    std::vector<string>::const_iterator it_end = int_arr.end();

    std::vector<string>::const_iterator it_upbound = 
        std::upper_bound(it, it_end, "1137", &compare);

    int i=0;
    for(; it!= it_upbound; it++) {
        std::cout << *it << std::endl;
        i++;
    }

    std::cout <<i <<std::endl;

    /*
    Options options;
    InternalKeyComparator icmp(options.comparator);
    RamTable * r1 = new RamTable(&icmp);
    std::vector<RamTable*> table_vector;
    RamTable * r2 = new RamTable(&icmp);

    InternalKey ikey1("237", 1, kTypeValue);
    InternalKey ikey2("1134", 2, kTypeValue);

    r1->Append(ikey1, "237");
    r2->Append(ikey2, "1134");

    table_vector.push_back(r1);

    std::vector<RamTable*>::const_iterator it = std::upper_bound()
    */
    return 0;
}