import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import create_engine, func, desc, join
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select, and_, or_, not_, text, table, literal_column, union



from config import config

db_url = 'localhost:5432'
db_name = 'postgres'
db_user = 'postgres'
db_password = '123'

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_url}/{db_name}')

metadata = MetaData()

users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String),
              Column('fullname', String))
addresses = Table('addresses', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('user_id', None, ForeignKey('users.id')),
                  Column('email_address', String, nullable=False)
)
cartest = Table('cartest', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String))
cartest2 = Table('cartest2', metadata,
                 Column('order_id', Integer, primary_key=True),
                 Column('location',String))

metadata.create_all(engine)

conn = engine.connect()
##conn.execute(addresses.insert(), [{'user_id': 1, 'email_address' : 'jack@yahoo.com'},
##                                  {'user_id': 1, 'email_address' : 'jack@msn.com'},
##                                  {'user_id': 2, 'email_address' : 'www@www.org'},
##                                  {'user_id': 2, 'email_address' : 'wendy@aol.com'},
##])
##ins = users.insert().values(name='jack', fullname='jack jones')
##result= conn.execute(ins)
##ins = users.insert()
##conn.execute(ins, id=2, name='wendy', fullname='wendy williams')
##s = select([users])
##result = conn.execute(s)
##for row in result:
##    print (row)
##row = result.fetchone()
##print ("name:", row['name'], "; fullname:", row['fullname'])
##result.close()
##s = select([users.c.name, users.c.fullname])
##result = conn.execute(s)
##for row in result:
##    print (row)
##for row in conn.execute(select([users, addresses])):
##    print (row)
##s = select([users, addresses]).where(users.c.id == addresses.c.user_id)
##for row in conn.execute(s):
##    print (row)
##print(users.c.id == addresses.c.user_id)
##print (users.c.id == 6)
##print (users.c.name + users.c.fullname)
##print (and_(
##    users.c.name.like('%j'),
##    users.c.id==addresses.c.user_id,
##    or_(
##        addresses.c.email_address=='wendy@aol.com',
##        addresses.c.email_address=='jack@yahoo.com'
##        ),
##    not_(users.c.id > 5)
##    )
##       )
##s = select([(users.c.fullname + ", "+ addresses.c.email_address).label('title')]).\
##    where(
##        and_(
##            users.c.id == addresses.c.user_id,
##            users.c.name.between('m', 'z'),
##            users.c.name == 'wendy',
##            or_(
##                addresses.c.email_address.like('%@aol.com'),
##                addresses.c.email_address.like('%@msn.com')
##                )
##            )
##        )
##print (conn.execute(s).fetchall())

##s = select([(users.c.fullname + ", " + addresses.c.email_address).
##            label('title')]).\
##            where(users.c.id == addresses.c.user_id).\
##            where(users.c.name.between('m', 'z')).\
##            where(
##                or_(
##                    addresses.c.email_address.like('%@aol.com'),
##                    addresses.c.email_address.like('%@msn.com')
##                    )
##                )
##print (conn.execute(s).fetchall())

##s = text(
##    "SELECT users.fullname || ', ' || addresses.email_address AS title "
##    "FROM users, addresses "
##    "WHERE users.id = addresses.user_id "
##    "AND users.name BETWEEN :x AND :y "
##    "AND (addresses.email_address LIKE :e1 "
##    "OR addresses.email_address LIKE :e2)")
##print (conn.execute(s, x='m', y='z', e1='%@aol.com', e2='%@msn.com').fetchall())

##s = text(
##    "SELECT users.fullname "
##    "FROM users, addresses "
##    "WHERE users.id = addresses.user_id "
##    "AND users.id = 2"
##    )
##
##print (conn.execute(s).fetchall())
##stmt = text("SELECT * from users WHERE users.name BETWEEN :x AND :y")
##stmt = stmt.bindparams(x="m", y="z")
##print (conn.execute(stmt).fetchall())

##
##s = select([
##    text("users.fullname || ', ' || addresses.email_address AS title")
##    ]).\
##    where(
##        and_(
##            text("users.id = addresses.user_id"),
##            text("users.name BETWEEN 'm' AND 'z'"),
##            text(
##                "(addresses.email_address LIKE :x "
##                "OR addresses.email_address LIKE :y)")
##            )
##        ).select_from(text('users, addresses'))
##print (conn.execute(s, x='%@aol.com', y='%@msn.com').fetchall())

##s = select([
##    literal_column("users.fullname", String) +
##    ", " +
##    literal_column("addresses.email_address").label("title")
##    ]).\
##        where(
##        and_(
##            literal_column("users.id") == literal_column("addresses.user_id"),
##            text("users.name BETWEEN 'm' AND 'z'"),
##            text(
##                "(addresses.email_address LIKE :x OR "
##                "addresses.email_address LIKE :y)")
##        )
##    ).select_from(table('users')).select_from(table('addresses'))
##print (conn.execute(s, x='%@aol.com', y='%@msn.com').fetchall())

##stmt = select([
##         addresses.c.user_id,
##         func.count(addresses.c.id).label('num_addresses')]).\
##         order_by(desc("num_addresses"))
##
##print (conn.execute(stmt).fetchall())

##print(users.join(addresses))

##print (users.join(addresses,
##                  addresses.c.email_address.like(users.c.name + '%')
##                  )
##       )

##s = select([users.c.fullname]).select_from(
##    users.join(addresses,
##               addresses.c.email_address.like(users.c.name + '%'))
##    )
##print (conn.execute(s).fetchall())

##s = select([cartest.c.name]).select_from(
##    cartest.join(cartest2,
##                 cartest.c.id==cartest2.c.order_id))
##print (conn.execute(s).fetchall())

##s = select([users.c.fullname]).select_from(users.outerjoin(addresses))
##print(s)

##j = cartest.join(cartest2,
##                 cartest.c.id == cartest2.c.order_id)
##stmt = select([cartest]).select_from(j)
##print (conn.execute(stmt).fetchall())

##u = union(
##    addresses.select().
##    where(addresses.c.email_address == 'foo@bar.com'),
##    addresses.select().
##    where(addresses.c.email_address.like('%@yahoo.com')),
##    )
##
##print (conn.execute(u).fetchall())

##stmt = select([users.c.name]).order_by(desc(users.c.name))
##print (conn.execute(stmt).fetchall())

##stmt = select([users.c.name, func.count(addresses.c.id)]).\
##       select_from(users.join(addresses)).\
##       group_by(users.c.name)
##print (conn.execute(stmt).fetchall())

stmt = select([users.c.name, func.count(addresses.c.id)]).\
       select_from(users.join(addresses)).\
       group_by(users.c.name).\
       having(func.length(users.c.name) > 2)
print (conn.execute(stmt).fetchall())
