version 1.0

workflow GetInputList
{
    input{
        Array[String] Patient_id
        Array[String] CTSeriesInstanceUID
        Array[String] RTSeriesInstanceUID
        Array[String] SGSeriesInstanceUID
    }
    call QueryInputs{
        input: Patient_id=Patient_id,
        CTSeriesInstanceUID=CTSeriesInstanceUID,
        RTSeriesInstanceUID=RTSeriesInstanceUID,
        SGSeriesInstanceUID=SGSeriesInstanceUID
    }

}
task QueryInputs
{
    input
    {
        Array[String] Patient_id
        Array[String] CTSeriesInstanceUID
        Array[String] RTSeriesInstanceUID
        Array[String] SGSeriesInstanceUID
    }
    command
    <<<
    echo "~{sep='||' Patient_id}"
    echo "~{sep='||' CTSeriesInstanceUID}"
    echo "~{sep='||'RTSeriesInstanceUID}"
    echo "~{sep='||'SGSeriesInstanceUID}"
    >>>
    meta
    {
        author: "Afshin"
        email: "akbarzadehm@gmail.com"
        description: "This workflow reads CT series instance uids and queries the data from dataset"
    }

}