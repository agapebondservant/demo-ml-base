from sqlalchemy import create_engine


# TODO: Generalize support to include Gemfire, RabbitMQ Streams
class WindowOffset:
    def last_offset(self):
        username, password, ipaddress, port, dbname = "gpadmin", "gpadmin", "", "", ""
        postgres_str = f"postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}"
        """
    "    username='gpadmin',password='gpadmin', \n",
    "    ipaddress='greenplum.greenplum-system.svc.cluster.local', port=5432, dbname='gpadmin'))\n",
    """
        cnx = create_engine(postgres_str)
        pd.read_sql_query('''SELECT * FROM madlib.pxf_clinical_data_000;''', cnx)
        return offset

    def set_offset(self, o: float):
        offset = o
