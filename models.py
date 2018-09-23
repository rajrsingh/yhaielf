import datetime
from sqlalchemy import Column, Text, Date, BigInteger, DateTime, Integer, Numeric, String, ARRAY, BOOLEAN, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

__name__ = 'models'

Base = declarative_base()

class User(Base):
        """Database model for users."""

        __tablename__ = 'users'
        id = Column(Text(), primary_key=True)
        active = Column(BOOLEAN(), default=True)
        balances = Column(JSON)
        balances_update = Column(DateTime())
        personal = Column(JSON, nullable=False)
        personal_update = Column(DateTime())
        income = Column(BigInteger())
        income_update = Column(DateTime())
        spending = Column(JSON)
        spending_update = Column(DateTime())
        goals = Column(JSON)
        goals_update = Column(DateTime())
        notices = Column(JSON)
        notices_update = Column(DateTime())
        categories = Column(JSON)
        categories_update = Column(DateTime())
        catrules = Column(JSON)
        catrules_update = Column(DateTime())
        created_on = Column(DateTime(), default=datetime.datetime.utcnow())

class Item(Base):
        """Database model for accounts at banks, credit cards, etc. (Plaid calls these items."""

        __tablename__ = 'items'
        user_id = Column(Text(), ForeignKey('users.id'), primary_key=True)
        item_id = Column(Text(), primary_key=True)
        access_token = Column(Text(), nullable=False)
        institution = Column(Text(), nullable=True)
        created_on = Column(DateTime(), default=datetime.datetime.utcnow())

class Transaction(Base):
    """Database model to track bank transactions."""
    
    __tablename__ = 'transactions'
    t_id = Column(Text(), primary_key=True)
    account_id = Column(Text(), nullable=False)
    item_id = Column(Text(), nullable=False)
    t_type = Column(Text())
    t_date = Column(DateTime())
    name = Column(Text())
    amount = Column(Numeric(12,2), nullable=False)
    category = Column(ARRAY(String))
    category_id = Column(Text())
    category_uid = Column(Text())
    address = Column(Text(), default=None)
    city = Column(Text(), default=None)
    state = Column(Text(), default=None)
    zipcode = Column(Text(), default=None)
    pending = Column(BOOLEAN(), default=False)
    pending_trans_id = Column(Text(), default=None)
    refnum = Column(Text())

class Logger(Base):
    """Database model to log all the things."""

    __tablename__ = 'applog'
    id = Column(Integer, primary_key=True)
    when = Column(DateTime(), default=datetime.datetime.utcnow())
    jsonmsg = Column(JSON)

class Perflog(Base):
    """Database model to log how long things take to run."""

    __tablename__ = 'perflog'
    id = Column(Integer, primary_key=True)
    starttime = Column(DateTime(), nullable=False)
    endtime = Column(DateTime(), nullable=False)
    method = Column(Text(), nullable=False)
    notes = Column(Text())

class ActualMonthSpend(Base):
    """Database model to track weekly spending by category"""

    __tablename__ = 'actualmonthspend'
    user_id = Column(Text(), ForeignKey('users.id'), primary_key=True)
    start_date = Column(DateTime(), nullable=False, primary_key=True)
    category_uid = Column(Text(), primary_key=True)
    amount = Column(Numeric(12,2), nullable=False)
    period = Column(Integer)

class AverageMonthSpend(Base):
    """Database model to track average spending per week of month"""

    __tablename__ = 'avgmonthspend'
    user_id = Column(Text(), ForeignKey('users.id'), primary_key=True)
    category_uid = Column(Text(), primary_key=True)
    amount = Column(Numeric(12,2), nullable=False)
    period = Column(Integer, primary_key=True)

class ActualMonthIncome(Base):
    """Database model to track weekly income"""

    __tablename__ = 'actualmonthincome'
    user_id = Column(Text(), ForeignKey('users.id'), primary_key=True)
    start_date = Column(DateTime(), nullable=False, primary_key=True)
    amount = Column(Numeric(12,2), nullable=False)
    period = Column(Integer)

class AverageMonthIncome(Base):
    """Database model to track average spending per week of month"""

    __tablename__ = 'avgmonthincome'
    user_id = Column(Text(), ForeignKey('users.id'), primary_key=True)
    amount = Column(Numeric(12,2), nullable=False)
    period = Column(Integer, primary_key=True)

class NoticeArchive(Base):
        """Database model for processed notices."""

        __tablename__ = 'noticearchive'
        id = Column(Integer, primary_key=True)
        user_id = Column(Text(), ForeignKey('users.id'))
        notice = Column(JSON)
        created_on = Column(DateTime(), default=datetime.datetime.utcnow())
