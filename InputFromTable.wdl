version 1.0

workflow GetInputList
{
    input{
        Array[String] patient_id
        Array[String] ct_seriesinstanceuid
        Array[String] rt_seriesinstanceuid
        Array[String] sg_seriesinstanceuid
        String json_file
    }
    call QueryInputs{
        input: patient_id=patient_id,
        ct_seriesinstanceuid=ct_seriesinstanceuid,
        rt_seriesinstanceuid=rt_seriesinstanceuid,
        sg_seriesinstanceuid=sg_seriesinstanceuid,
        json_file=json_file
    }

}
task QueryInputs
{
    input
    {
        Array[String] patient_id
        Array[String] ct_seriesinstanceuid
        Array[String] rt_seriesinstanceuid
        Array[String] sg_seriesinstanceuid
        String json_file
    }
    command
    <<<
    python3 <<
    CODE
    def query_and_write(json_file_name: str,
                    input_var_name: str,
                    ct_uid: list,
                    rt_uid: list,
                    sg_uid: list):
    cond_ct = 'WHERE SERIESINSTANCEUID IN {}'.format(tuple(ct_uid))
    cond_rt = 'WHERE SERIESINSTANCEUID IN {}'.format(tuple(rt_uid))
    cond_sg = 'WHERE SERIESINSTANCEUID IN {}'.format(tuple(sg_uid))
    query = """
    WITH
        CT_SERIES AS 
        (
            SELECT
                PATIENTID,
                SERIESINSTANCEUID AS CTSERIESINSTANCEUID,
                ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_CT,
            FROM
                `{0}`
            {1}
            GROUP BY PATIENTID, SERIESINSTANCEUID
        ),
        RTSTRUCT_SERIES AS 
        (
            SELECT
                (PATIENTID),
                SERIESINSTANCEUID AS RTSTRUCTSERIESINSTANCEUID,
                ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_RT,
            FROM
                `{0}`
            {2}
            GROUP BY PATIENTID, SERIESINSTANCEUID
        ),
        SEG_SERIES AS 
        (
            SELECT
                (PATIENTID),
                SERIESINSTANCEUID AS SEGSERIESINSTANCEUID,
                ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_SG,
            FROM
                `{0}`
            {3}
            GROUP BY PATIENTID, SERIESINSTANCEUID
        )
    SELECT
        PATIENTID,
        CTSERIESINSTANCEUID,
        INPUT_CT,
        RTSTRUCTSERIESINSTANCEUID,
        INPUT_RT,
        SEGSERIESINSTANCEUID,
        INPUT_SG
    FROM CT_SERIES JOIN RTSTRUCT_SERIES USING (PATIENTID)
    JOIN SEG_SERIES USING (PATIENTID)
    ORDER BY PATIENTID
    """.format(
        'canceridc-data.idc_views.dicom_all',
        cond_ct,
        cond_rt,
        cond_sg
        )
    print(query)
    client = bigquery.Client()
    query_job = client.query(query)
    q_results = query_job.result()
    content = ''
    size_limit = 10000000
    if q_results is not None:
        content += (
            'workspace:PATIENTID' + '\t' +
            'CTSERIESINSTANCEUID' + '\t' +
            'INPUT_CT' + '\t' +
            'RTSTRUCTSERIESINSTANCEUID' + '\t' +
            'INPUT_RT' + '\t' +
            'SEGSERIESINSTANCEUID' + '\t' +
            'INPUT_SG'
        )
        file_counter = 0
        vec_data = []
        sz_factor = 1
        for i, row in enumerate(q_results):
            data1 = {}
            data1["PATIENTID"] = row.PATIENTID
            data1["CTSERIESINSTANCEUID"] = row.CTSERIESINSTANCEUID
            data1["INPUT_CT"] = row.INPUT_CT
            data1["RTSTRUCTSERIESINSTANCEUID"] = row.RTSTRUCTSERIESINSTANCEUID
            data1["INPUT_RT"] = row.INPUT_RT
            data1["SEGSERIESINSTANCEUID"] = row.SEGSERIESINSTANCEUID
            data1["INPUT_SG"] = row.INPUT_SG
            vec_data.append(data1)
            size = len(
                json.dumps({input_var_name: vec_data}, indent=4)) * sz_factor
            size_1 = len(
                json.dumps({input_var_name: vec_data[0:-1]}, indent=4)) * sz_factor            
            if size > 0.99 * size_limit:
                filename = '{}_{:03d}.json'.format(
                    json_file_name, file_counter)
                with open(filename, 'w') as fp:
                    json.dump(
                        {input_var_name: vec_data[0:-1]}, fp, indent=4)
                sz = os.path.getsize(filename)
                sz_factor = sz / size_1
                file_counter += 1
                vec_data = [vec_data[-1]]
        
        filename = '{}_{:03d}.json'.format(json_file_name, file_counter)
        with open(filename, 'w') as fp:
            json.dump(
                {input_var_name: vec_data}, fp, indent=4)

    j_file_name = '~{json_file}'
    p_id = ['~{sep="\', \'"  patient_id}']
    ct_uid = ['~{sep="\', \'"  ct_seriesinstanceuid}']
    rt_uid = ['~{sep="\', \'" rt_seriesinstanceuid}']
    sg_uid = ['~{sep="\', \'" sg_seriesinstanceuid}']
    var_name = 'data'
    query_and_write(j_file_name, var_name, ct_uid, rt_uid, sg_uid)
    CODE
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