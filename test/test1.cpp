#include <vector>
#include <iostream>
#include <algorithm>


using namespace std;

void print(int ** arr, int len) {
    for(int i=0; i < len; i++) {
        cout << *(arr[i]) << endl;
    }
}

class people {
public:
    people() {

    }


    void pushCars(int level, int *car_number) {
        cars[level].push_back(car_number);
    }

    void PrintCarsNumber() {
        for(int level=0; level < 2; level++) {
            for(int i=0; i< cars[level].size(); i++) {
                cout<< *cars[level][i] << " ";
            }
            cout<< endl;
        }

        cout<< endl;
    }

private:
    vector<int *> cars[2];
};

int main() {
    people * p1 = new people();
    int *a = new int(7);
    int *b = new int(8);
    p1->pushCars(0, a);
    p1->pushCars(1, b);
    people *p2 = new people(*p1);

    p2->PrintCarsNumber();

    return 0;
}