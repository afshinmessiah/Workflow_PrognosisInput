version 1.0

workflow GetInputList
{
    input{
        String Patient_id
        String CTSeriesInstanceUID
        String RTSeriesInstanceUID
        String SGSeriesInstanceUID
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
        String Patient_id
        String CTSeriesInstanceUID
        String RTSeriesInstanceUID
        String SGSeriesInstanceUID
    }
    command
    <<<
    echo "~{Patient_id}"
    echo "~{CTSeriesInstanceUID}"
    echo "~{RTSeriesInstanceUID}"
    echo "~{SGSeriesInstanceUID}"
    >>>
    meta
    {
        author: 'Afshin'
        email: 'akbarzadehm@gmail.com'
        description:'This workflow reads CT series instance uids and queries the data from dataset'
    }

}