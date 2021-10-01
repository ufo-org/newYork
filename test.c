
#include <stdio.h>

#include "target/nyc.h"

#define megs(n) (n  * 1000ll * 1000ll)


 
int32_t populate(BoroughPopulateData unused, uintptr_t start, uintptr_t end, unsigned char* target){
    uint64_t* writeTo = (uint64_t*) target;
    printf("populating %lu - %lu\n", start, end);
    for(uintptr_t i = start; i < end; i++){
        writeTo[i - start] = ~i;
    }

    return 0;
}

void main(){

    NycCore core = nyc_new_core("/tmp/", megs(500), megs(1000));
    if(nyc_is_error(&core)){
        fprintf(stderr, "no core\n");
        exit(-1);
    }

    #define elementCt 100000000l

    BoroughParameters params = (struct BoroughParameters) {
        .element_ct = elementCt,
        .element_size = sizeof(uint64_t),
        .min_load_ct = 1000000,
        .populate_data = NULL,
        .populate_fn = &populate,
        .header_size = 0,
    };

    Borough b = nyc_new_borough(&core, &params);
    if(borough_is_error(&b)){
        fprintf(stderr, "bad neighborhood\n");
        exit(-2);
    }

    uint64_t readBuffer = 17;
    borough_write(&b, 0, &readBuffer);

    for(uint64_t i = 1; i < elementCt; i++){
        // if(0 == i % 1000)
        //   printf("%lu / %lu\n", i, elementCt);
        int result = borough_read(&b, i, &readBuffer);
        if(result != 0){
            fprintf(stderr, "read failed\n");
            exit(-3);
        }

        if(~i != readBuffer){
            fprintf(stderr, "bad read, expected %lu got %lu\n", ~i, readBuffer);
            exit(-4);
        }
    }

    int result = borough_read(&b, 0, &readBuffer);
    if(result != 0){
        fprintf(stderr, "read failed\n");
        exit(-5);
    }

    if(17 != readBuffer){
        fprintf(stderr, "bad read, expected %u got %lu\n", 17, readBuffer);
        exit(-6);
    }

    borough_free(b);

    nyc_shutdown(core);
}