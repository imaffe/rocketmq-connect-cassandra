from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
from sqlalchemy.orm import sessionmaker
import argparse






Base = declarative_base()
class JdbcCassandra(Base):
    __tablename__ = 'jdbc_cassandra'
    int_type = Column(Integer, primary_key=True, autoincrement=True)
    # ascii_type = Column() ,
    # boolean_type = Column(),
    # date_type = Column(),
    # decimal_type = Column(),
    # double_type = Column(),
    # float_type = Column(),
    # inet_type = Column(),
    # smallint_type = Column(),
    # time_type  = Column(),
    # text_type = Column(),
    # timestamp_type  = Column(),
    # timeuuid_type  = Column(),
    # tinyint_type  = Column(),
    # uuid_type  = Column(),
    # varchar_type  = Column(),
    # varint_type  = Column(),
    
def main():
    # define parser
    parser = argparse.ArgumentParser()
    parser.add_argument('hostname', metavar='HOSTNAME', type=str,
                        help='hostname of target mysql database ')
    parser.add_argument('port', metavar='PORT', type=str,
                        help='port of target mysql database')
    parser.add_argument('username', metavar='USERNAME', type=str,
                        help='username of the user connecting to mysql')
    parser.add_argument('password', metavar='PASSWORD', type=str,
                        help='password of specified user')   
    parser.add_argument('database', metavar='DATABASE', type=str,
                        help='which database to connect to') 
    parser.add_argument('count', metavar='COUNT', type=int,
                        help='how many random records to insert into the database')                                                           
    args = parser.parse_args()


    # get variabless from command line
    hostname = args.hostname
    port = args.port
    username = args.username
    password = args.password
    count = args.count


    # create db connection
    engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format(username, password, hostname, port), echo=True)


    # create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    for i in range(0, count):
        random_record = JdbcCassandra()
        session.add(new_profile)

    session.commit()


if __name__ == "__main__":
    main()