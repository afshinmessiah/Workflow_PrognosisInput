version 1.0

workflow GetInputList
{
    input{
        Array[String] patient_id
        Array[String] CTSERIESINSTANCEUID
        Array[String] RTSERIESINSTANCEUID
        Array[String] SGSERIESINSTANCEUID
    }
    call QueryInputs{
        input: patient_id=patient_id,
        CTSERIESINSTANCEUID=CTSERIESINSTANCEUID,
        RTSERIESINSTANCEUID=RTSERIESINSTANCEUID,
        SGSERIESINSTANCEUID=SGSERIESINSTANCEUID
    }

}
task QueryInputs
{
    input
    {
        Array[String] patient_id
        Array[String] CTSERIESINSTANCEUID
        Array[String] RTSERIESINSTANCEUID
        Array[String] SGSERIESINSTANCEUID
    }
    command
    <<<
    echo "~{sep='||' patient_id}"
    echo "~{sep='||' CTSERIESINSTANCEUID}"
    echo "~{sep='||'RTSERIESINSTANCEUID}"
    echo "~{sep='||'SGSERIESINSTANCEUID}"
    >>>
    runtime
    {
        # docker: "biocontainers/plastimatch:v1.7.4dfsg.1-2-deb_cv1"
        docker: "afshinmha/plastimatch_terra_00:terra_run00"
        memory: "1GB"

    }
    meta
    {
        author: "Afshin"
        email: "akbarzadehm@gmail.com"
        description: "This workflow reads CT series instance uids and queries the data from dataset"
    }

}