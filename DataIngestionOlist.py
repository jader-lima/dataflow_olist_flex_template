from __future__ import absolute_import
import argparse
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions



class DataIngestionOlist:
    def textotodict(self, string_input):
        values = re.split(",",re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        values = (values[8][0:10],values[0],values[7],values[8][0:10],values[9][0:10],values[10][0:10],values[11][0:10],values[12][0:10],
            values[1],values[2],values[3],values[4],values[13],values[14],values[15],values[16],values[20][0:10],values[27],
            values[28],values[21],values[22],values[23],values[24],values[25],values[26])  
       
#        values = (values[8],values[0],values[7],values[8],values[9],values[10],values[11],values[12],values[1],values[2],values[3],values[4],values[13],values[14],values[15],values[16],values[20],values[27],values[28],values[21],values[22],values[23],values[24],values[25],values[26])     

        row = dict(
            zip(('partition_date','order_id','order_status','order_purchase_timestamp','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date','payment_sequential','payment_type','payment_installments','payment_value','review_score','customer_id','customer_city','customer_state','shipping_limit_date','product_id','product_category_name','itens_count','price','freight_value','seller_id','seller_city','seller_state'), 
                values))
                
        return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    parser.add_argument('--input', dest='input', required=False,    help='Input file or files')
   
    parser.add_argument('--bq_table_id', dest='bq_table_id', required=False, help='Big query table to write the data.')
 
    path_args, pipeline_args = parser.parse_known_args(argv)
    
    opts = PipelineOptions(pipeline_args)
    
    data_ingestion = DataIngestionOlist()
  
    p1 = beam.Pipeline(options=opts)

    data = (
        p1 
        | 'Read from a File' >> beam.io.ReadFromText(path_args.input,  skip_header_lines=1)    
        | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.textotodict(s))
        | "Write to BigQuery Table" >> beam.io.WriteToBigQuery(table=path_args.bq_table_id,
                                                schema='partition_date:DATE,order_id:STRING,order_status:STRING,order_purchase_timestamp:DATE,order_approved_at:DATE,order_delivered_carrier_date:DATE,order_delivered_customer_date:DATE,order_estimated_delivery_date:DATE,payment_sequential:INTEGER,payment_type:STRING,payment_installments:INTEGER,payment_value:FLOAT,review_score:STRING,customer_id:STRING,customer_city:STRING,customer_state:STRING,shipping_limit_date:DATE,product_id:STRING,product_category_name:STRING,itens_count:INTEGER,price:FLOAT,freight_value:FLOAT,seller_id:STRING,seller_city:STRING,seller_state:STRING', 
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,             
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
        )


    p1.run().wait_until_finish()


if __name__ == '__main__':
    run()
