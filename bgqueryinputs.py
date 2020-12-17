from google.cloud import bigquery
import os
import json

def query_and_write(json_file_name: str,
                    input_var_name: str,
                    pat_number: int = -1):
    query = """
    WITH
        CT_SERIES AS 
        (
            SELECT
                PATIENTID,
                STUDYINSTANCEUID AS CTSTUDYINSTANCEUID,
                SERIESINSTANCEUID AS CTSERIESINSTANCEUID,
                ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_CT,
            FROM
                `{0}`
            WHERE
                SOURCE_DOI = "10.7937/K9/TCIA.2015.PF0M9REI"
                AND MODALITY = "CT"
            GROUP BY PATIENTID, STUDYINSTANCEUID, SERIESINSTANCEUID
        ),
        RTSTRUCT_SERIES AS 
        (
            SELECT
                (PATIENTID),
                STUDYINSTANCEUID AS RTSTRUCTSTUDYINSTANCEUID,
                SERIESINSTANCEUID AS RTSTRUCTSERIESINSTANCEUID,
                ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_RT,
            FROM
                `{0}`
            WHERE
                SOURCE_DOI = "10.7937/K9/TCIA.2015.PF0M9REI"
                AND MODALITY = "RTSTRUCT"
            GROUP BY PATIENTID, STUDYINSTANCEUID, SERIESINSTANCEUID
        ),
        SEG_SERIES AS 
        (
            SELECT
                (PATIENTID),
                STUDYINSTANCEUID AS SEGSTUDYINSTANCEUID,
                SERIESINSTANCEUID AS SEGSERIESINSTANCEUID,
                ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_SG,
            FROM
                `{0}`
            WHERE
                SOURCE_DOI = "10.7937/K9/TCIA.2015.PF0M9REI"
                AND MODALITY = "SEG"
            GROUP BY PATIENTID, STUDYINSTANCEUID, SERIESINSTANCEUID
        )
    SELECT
        PATIENTID,
        CTSTUDYINSTANCEUID,
        CTSERIESINSTANCEUID,
        INPUT_CT,
        RTSTRUCTSTUDYINSTANCEUID,
        RTSTRUCTSERIESINSTANCEUID,
        INPUT_RT,
        SEGSTUDYINSTANCEUID,
        SEGSERIESINSTANCEUID,
        INPUT_SG
    FROM CT_SERIES JOIN RTSTRUCT_SERIES USING (PATIENTID)
    JOIN SEG_SERIES USING (PATIENTID)
    ORDER BY PATIENTID
    {1}
    """.format(
        'canceridc-data.idc_views.dicom_all',
        '' if pat_number < 1 else 'LIMIT {}'.format(pat_number))
    # print(query)
    client = bigquery.Client()
    query_job = client.query(query)
    q_results = query_job.result()
    content = ''
    size_limit = 10000000
    if q_results is not None:
        content += (
            'workspace:PATIENTID' + '\t' +
            'CTSTUDYINSTANCEUID' + '\t' +
            'CTSERIESINSTANCEUID' + '\t' +
            'INPUT_CT' + '\t' +
            'RTSTRUCTSTUDYINSTANCEUID' + '\t' +
            'RTSTRUCTSERIESINSTANCEUID' + '\t' +
            'INPUT_RT' + '\t' +
            'SEGSTUDYINSTANCEUID' + '\t' +
            'SEGSERIESINSTANCEUID' + '\t' +
            'INPUT_SG'
        )
        content_form = (
            '{}\t' +
            '{}\t' +
            '{}\t' +
            '{}\t' +
            '{}\t' +
            '{}\t' +
            '{}\t' +
            '{}\t' +
            '{}\t' +
            '{}\t'
        )
        file_counter = 0
        vec_data = []
        sz_factor = 1
        for i, row in enumerate(q_results):
            content += content_form.format(
                row.PATIENTID,
                row.CTSTUDYINSTANCEUID,
                row.CTSERIESINSTANCEUID,
                row.INPUT_CT,
                row.RTSTRUCTSTUDYINSTANCEUID,
                row.RTSTRUCTSERIESINSTANCEUID,
                row.INPUT_RT,
                row.SEGSTUDYINSTANCEUID,
                row.SEGSERIESINSTANCEUID,
                row.INPUT_SG
            )
            data1 = {}
            data1["PATIENTID"] = row.PATIENTID
            # data1["CTSTUDYINSTANCEUID"] = row.CTSTUDYINSTANCEUID
            data1["CTSERIESINSTANCEUID"] = row.CTSERIESINSTANCEUID
            # data1["INPUT_CT"] = row.INPUT_CT
            # data1["RTSTRUCTSTUDYINSTANCEUID"] = row.RTSTRUCTSTUDYINSTANCEUID
            data1["RTSTRUCTSERIESINSTANCEUID"] = row.RTSTRUCTSERIESINSTANCEUID
            # data1["INPUT_RT"] = row.INPUT_RT
            # data1["SEGSTUDYINSTANCEUID"] = row.SEGSTUDYINSTANCEUID
            data1["SEGSERIESINSTANCEUID"] = row.SEGSERIESINSTANCEUID
            # data1["INPUT_SG"] = row.INPUT_SG
            # for i in range(0, len(data1["INPUT_CT"])):
            #     data1["INPUT_CT"][i] = data1["INPUT_CT"][i].replace(
            #         'idc-tcia-1-nsclc-radiomics', "idc-tcia-nsclc-radiomics")
            # for i in range(0, len(data1["INPUT_RT"])):
            #     data1["INPUT_RT"][i] = data1["INPUT_RT"][i].replace(
            #         'idc-tcia-1-nsclc-radiomics', "idc-tcia-nsclc-radiomics")
            # for i in range(0, len(data1["INPUT_SG"])):
            #     data1["INPUT_SG"][i] = data1["INPUT_SG"][i].replace(
            #         'idc-tcia-1-nsclc-radiomics', "idc-tcia-nsclc-radiomics")
            vec_data.append(data1)
            size = len(
                json.dumps({input_var_name: vec_data}, indent=4)) * sz_factor
            size_1 = len(
                json.dumps({input_var_name: vec_data[0:-1]}, indent=4)) * sz_factor
            # if i == 0:
            #     with open('test.json', 'w') as fp:
            #         json.dump({input_var_name: vec_data}, fp, indent=4)
            #     sz = os.path.getsize('test.json')
            #     sz_factor = sz / size
            #     size = sz
            #     os.remove('test.json')

            
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
        
        with open('x.tsv', 'w') as fp:
            content = writeInTsvFile(vec_data)
            print(content)
            fp.write(content)
            


            
j_file_name = 'ss'
var_name = 'vv'
lim = -1
query_and_write(j_file_name, var_name, lim)