{% macro snowflake__compile_py_code(model, py_code, output_table) %}
import snowflake.snowpark

{{ py_code }}

def materialize(session, df, target_relation):
    if not isinstance(df, snowflake.snowpark.DataFrame):
        df = session.create_dataframe(df)
    df.write.mode('overwrite').save_as_table(target_relation, table_type='temporary')

def main(session):
    ref = session.table
    model_df = ref('{{ model }}')
    output_df = test(model_df, ref, session)
    materialize(session, output_df, '{{ output_table }}')
{% endmacro %}


{% macro bigquery__compile_py_code(model, py_code, output_table) %}
import pyspark.sql

{{ py_code }}

def materialize(session, df, target_relation):
    if not isinstance(df, pyspark.sql.DataFrame):
        df = session.createDataFrame(df)
    df.write.mode('overwrite').format('bigquery').option('writeMethod', 'direct').option("writeDisposition", 'WRITE_TRUNCATE').save(target_relation)

def get_session():
    session = pyspark.sql.SparkSession.builder.appName('Elementary').getOrCreate()
    session.conf.set('viewsEnabled', 'true')
    session.conf.set('temporaryGcsBucket', '{{ target.gcs_bucket }}')
    return session

def main():
    session = get_session()
    ref = session.read.format("bigquery").load
    model_df = ref('{{ model }}')
    output_df = test(model_df, ref, session)
    materialize(session, output_df, '{{ output_table }}')

main()
{% endmacro %}
