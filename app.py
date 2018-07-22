import os, datetime, time, json, schedule
from dateutil.relativedelta import relativedelta
from sqlalchemy.sql import label
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.attributes import flag_modified
from models import *

engine = create_engine('postgresql+psycopg2://%s:%s@%s/%s' % (os.environ['DBUSER'], os.environ['DBPASS'], os.environ['DBHOST'], os.environ['DBNAME']))
MONTHS_MEASURED = 4

def mkDateTime(dateString,strFormat="%Y-%m-%d"):
  # Expects "YYYY-MM-DD" string
  # returns a datetime object
  eSeconds = time.mktime(time.strptime(dateString,strFormat))
  return datetime.datetime.fromtimestamp(eSeconds)

def formatDate(dtDateTime,strFormat="%Y-%m-%d"):
  # format a datetime object as YYYY-MM-DD string and return
  return dtDateTime.strftime(strFormat)

def mkFirstOfMonth(dtDateTime):
  return mkDateTime(formatDate(dtDateTime,"%Y-%m-01"))

def mkFirstOfNextMonth(dtDateTime):
  last = mkLastOfMonth(dtDateTime)
  first = last + datetime.timedelta(seconds=1) # add one second
  return first

def mkLastOfMonth(dtDateTime):
  dYear = dtDateTime.strftime("%Y")        #get the year
  dMonth = str(int(dtDateTime.strftime("%m"))%12+1)#get next month, watch rollover
  dDay = "1"                               #first day of next month
  if dMonth == '1':
    dYear = str(int(dYear)+1)
  nextMonth = mkDateTime("%s-%s-%s"%(dYear,dMonth,dDay))#make a datetime obj for 1st of next month
  delta = datetime.timedelta(seconds=1)    #create a delta of 1 second
  return nextMonth - delta                 #subtract from nextMonth and return

def mkDayBreaks(dt):
  daybreaks = []
  daybreaks.append( mkFirstOfMonth(dt) )
  daybreaks.append( mkFirstOfMonth(dt) + datetime.timedelta(days=8) )
  daybreaks.append( mkFirstOfMonth(dt) + datetime.timedelta(days=16) )
  daybreaks.append( mkFirstOfMonth(dt) + datetime.timedelta(days=24) )
  daybreaks.append( mkFirstOfNextMonth(dt) )
  return daybreaks

def getsession():
  Session = sessionmaker(bind=engine)
  return Session()

def applog(json_msg):
  session = getsession()
  dbmsg = Logger(jsonmsg=json_msg)
  session.add(dbmsg)
  session.commit()
  session.close()


def compute_expenses(userid, session):
  session = getsession()

  ## get oldest transaction date
  # t = session.query(Transaction).order_by(Transaction.t_date.asc()).limit(1).first()
  # startdate = mkFirstOfNextMonth(t.t_date)
  # get latest transaction date
  t = session.query(Transaction).order_by(Transaction.t_date.desc()).limit(1).first()
  endate = (t.t_date)
  ## get date 4 months ago
  startdate = mkFirstOfMonth(endate) - relativedelta(months=MONTHS_MEASURED)


  item_ids = []
  itemrecs = session.query(Item).filter(Item.user_id.like(userid)).all()
  for ir in itemrecs:
    item_ids.append(ir.item_id)

  daybreaks = mkDayBreaks(startdate)
  while daybreaks[4] < endate:
    for i in range(4):
      queries = []
      queries.append( Transaction.t_date >= daybreaks[i] )
      queries.append( Transaction.t_date < daybreaks[i+1] )
      queries.append( Transaction.amount > 0 )
      queries.append( Transaction.item_id.in_(item_ids) )
      # ignore transfers and credit card payments (that would be double-counting)
      queries.append( Transaction.category_uid.notin_(["21001000","21006000","16001000"]) )
      q = session.query(Transaction.category_uid, func.sum(Transaction.amount))
      ts = q.filter(*queries).group_by(Transaction.category_uid).all()
      totamt = 0
      for t in ts:
        totamt += t[1]
        a = ActualMonthSpend(user_id=userid, start_date=daybreaks[i], amount=t[1], category_uid=t[0], period=i+1)
        session.merge(a)
      
      # print( formatDate(daybreaks[i])+": "+str(totamt))
      ## category_uid = 00000000 is special 'all categories' category
      a = ActualMonthSpend(user_id=userid, start_date=daybreaks[i], amount=totamt, category_uid="00000000", period=i+1)
      session.merge(a)

    daybreaks = mkDayBreaks(daybreaks[4])
    session.commit()
  return True

def compute_projected_spend(userid, session):
  ## delete old data
  session.commit()
  session.query(AverageMonthSpend).filter(AverageMonthSpend.user_id.like(userid)).delete(synchronize_session='fetch')
  session.commit()

  # get latest transaction date
  t = session.query(Transaction).order_by(Transaction.t_date.desc()).limit(1).first()
  endate = (t.t_date)
  four_months_ago = mkFirstOfMonth(endate) - relativedelta(months=MONTHS_MEASURED)

  # get all categories user spent on in the past four months
  queries = []
  queries.append( ActualMonthSpend.user_id.like(userid) )
  queries.append( ActualMonthSpend.start_date >= four_months_ago )
  q = session.query(ActualMonthSpend.category_uid, func.count(ActualMonthSpend.category_uid))
  spendingdata = q.filter(*queries).group_by(ActualMonthSpend.category_uid).all()
  # initialize spending for each category to 0
  spending = {}
  for r in spendingdata:
    spending[r[0]] = 0

  ## TODO describe
  # month_weights_template = [1, 2, 3, 4]
  # WEIGHTED_PERIODS = sum(month_weights_template)
  # month_weights = {}
  # currdt = four_months_ago
  # for i in range(len(month_weights_template)):
  #   month_weights[currdt.strftime("%m")] = month_weights_template[i]
  #   currdt = currdt + relativedelta(months=1)
  # print (month_weights)

  ## add up all spending in each period over MONTHS_MEASURED, then divide by MONTHS_MEASURED
  for PERIOD in range(1, 5):
    period_spending = dict(spending)
    # get all expenses (from aggregated table) user spent on in the past four months
    queries = []
    queries.append( ActualMonthSpend.user_id.like(userid) )
    queries.append( ActualMonthSpend.period == PERIOD )
    queries.append( ActualMonthSpend.start_date >= four_months_ago )
    spendingdata = session.query(ActualMonthSpend).filter(*queries).all()
    for expense in spendingdata:
      # calculate WEIGHT based on date
      # wt = month_weights[expense.start_date.strftime("%m")]
      period_spending[expense.category_uid] += expense.amount / MONTHS_MEASURED #round( (expense.amount * wt) / WEIGHTED_PERIODS, 2)
    for k, v in period_spending.items():
      v = round(v, 2)
      s = AverageMonthSpend(user_id=userid, category_uid=k, amount=v, period=PERIOD)
      session.add(s)
  session.commit()
  return True

def projected_spend_to_budgets(userid, session):
  user = session.query(User).get(userid)
  spending = user.spending
  ## build a list of categories in budgets to skip when computing budget for other
  budget_categories = []
  for budget in spending['budgets']:
    if budget['name'] != 'Other':
      budget_categories.extend(budget['categories'])
  budget_categories.append('00000000') # the special category representing total spending for period

  ## loop through each budget, get it's categories, query avgmonthspend for 
  ## avg spending in those categories, save avg spending per budget per period
  projectedspending = []
  for budget in spending['budgets']:
    # print("BUDGET: "+budget['name'])
    # print("categories: "+str(budget['categories']))
    projectedspend = { 'name': budget['name'] }
    # for PERIOD in range(1, 5):
    queries = []
    queries.append( AverageMonthSpend.user_id.like(userid) )
    # queries.append( AverageMonthSpend.period == PERIOD )
    if budget['name'] == 'Other':
      queries.append( AverageMonthSpend.category_uid.notin_(budget_categories) )
    else:
      queries.append( AverageMonthSpend.category_uid.in_(budget['categories']) )
    q = session.query(AverageMonthSpend.period, func.sum(AverageMonthSpend.amount))
    spendingdata = q.filter(*queries).group_by(AverageMonthSpend.period).order_by(AverageMonthSpend.period.asc()).all()
    ps = []
    for spend in spendingdata:
      # print(spend)
      ps.append(int(spend[1]))
    projectedspend['amounts'] = ps
    projectedspending.append(projectedspend)
  spending['projectedspend'] = projectedspending
  # print (spending)
  user.spending = spending
  flag_modified(user, "spending")
  session.commit()

def expense_job():
  session = getsession()
  # userid = 'auth0|5b021d905d7d1617fd7dfadb'
  for user in session.query(User):
    userid = user.id
    success = compute_expenses(userid, session)
    success = compute_projected_spend(userid, session)
    success = projected_spend_to_budgets(userid, session)
  session.close()
  applog({"msg":"success", "service":"aielf", "function":"expense_job"})

# expense_job()
schedule.every().day.at("11:30").do(expense_job)

while True:
  schedule.run_pending()
  time.sleep(100)