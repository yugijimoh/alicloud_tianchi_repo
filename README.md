# alicloud_tianchi_repo


## Flask entrance: entrance.py
    Main functions:
    /ready: health check
    /setParameter() set port number and trigger the applications for port 8000 and 8001

## Client pod: run_client.py

## Backend pod: run_checksum.py
####global variable: 
`error_spans`: {traceid1:[span1,span2...spann],traceid2:[span1,span2...spann]}
`res_dict`: {traceid1:md5value1, traceid2:md5value2...traceidn:md5valuen} 
#### methods: 
* update_error_dict_with_trace_from_client (`data`)<br>
`data`: error_traces [{ trace_id:spans[span1,span2...spann] }
    
* sort_and_checksum_spans()<br>

* send_checksum()<br>

* get_md5(`span_string`)<br>
`span_string`: span list data of a trace id, transformed into string with delimiter `\n`