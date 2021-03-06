import os, datetime, time, json, schedule, random
from dateutil.relativedelta import relativedelta
from sqlalchemy.sql import label
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.attributes import flag_modified
from models import *
from samplegoals import *
from notices_inspire import *

engine = create_engine('postgresql+psycopg2://%s:%s@%s/%s' % (os.environ['DBUSER'], os.environ['DBPASS'], os.environ['DBHOST'], os.environ['DBNAME']))
MONTHS_MEASURED = 4
from pillarconstants import *

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

def applog(json_msg, session):
  """ make sure you have a running session """
  dbmsg = Logger(jsonmsg=json_msg)
  session.add(dbmsg)
  session.commit()

def compute_expenses(userid, session):
  """ Compute spending per period over the last 4 months for every transaction category and save to DB """

  # get all the user's item ids
  item_ids = []
  itemrecs = session.query(Item).filter(Item.user_id.like(userid)).all()
  if itemrecs is None:
    return
  for ir in itemrecs:
    item_ids.append(ir.item_id)
  if len(item_ids) < 1:
    return

  ## get oldest transaction date
  # t = session.query(Transaction).order_by(Transaction.t_date.asc()).limit(1).first()
  # startdate = mkFirstOfNextMonth(t.t_date)
  # get latest transaction date
  t = session.query(Transaction).filter(Transaction.item_id.in_(item_ids)).order_by(Transaction.t_date.desc()).limit(1).first()
  if t is None:
    return
  endate = (t.t_date)
  ## get date 4 months ago
  startdate = mkFirstOfMonth(endate) - relativedelta(months=MONTHS_MEASURED)

  daybreaks = mkDayBreaks(startdate) # [1st, 9th, 17th, 25th, 1st]
  while daybreaks[4] <= endate: # handle a month at a time
    for i in range(4): # handle a pillarperiod at a time
      queries = []
      queries.append( Transaction.t_date >= daybreaks[i] )
      queries.append( Transaction.t_date < daybreaks[i+1] )
      queries.append( Transaction.amount > 0 )
      queries.append( Transaction.item_id.in_(item_ids) )
      # ignore transfers and credit card payments (that would be double-counting)
      # also ignore spending on goals
      queries.append( Transaction.category_uid.notin_(["21001000","21006000","16001000",SPECIAL_GOAL_SPEND]) )
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
  """ Compute average spending per period for every transaction category and save to DB """

  ## delete old data
  session.commit()
  session.query(AverageMonthSpend).filter(AverageMonthSpend.user_id.like(userid)).delete(synchronize_session='fetch')
  session.commit()

  # get latest transaction date
  t = session.query(Transaction).order_by(Transaction.t_date.desc()).limit(1).first()
  if t is None:
    return
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
  """ Aggregate average spending per category, per period up to the categories in the user's budgets """

  user = session.query(User).get(userid)
  if not user.spending or not 'budgets' in user.spending:
    return False
  spending = user.spending
  ## build a list of categories in budgets to skip 
  ## when computing budget for Unbudgeted
  budget_categories = [SPECIAL_GOAL_SPEND]
  for budget in spending['budgets']:
    if budget['name'] != 'Unbudgeted':
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
    if budget['name'] == 'Unbudgeted':
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
  user.spending_update = datetime.datetime.today()
  flag_modified(user, "spending")
  session.commit()

def compute_income(userid, session):
  """ calculate user's monthly and pillarperiod income """
  t = session.query(Transaction).order_by(Transaction.t_date.desc()).limit(1).first()
  if t is None:
    return
  endate = (t.t_date)
  ##get date 4 months ago
  startdate = mkFirstOfMonth(endate) - relativedelta(months=MONTHS_MEASURED)

  # get all the user's item ids
  item_ids = []
  itemrecs = session.query(Item).filter(Item.user_id.like(userid)).all()
  if itemrecs is None:
    return
  for ir in itemrecs:
    item_ids.append(ir.item_id)
  if len(item_ids) < 1:
    return

  allincomes = []
  daybreaks = mkDayBreaks(startdate) # [1st, 9th, 17th, 25th, 1st]
  while daybreaks[4] <= endate: # handle a month at a time
    periodincomes = []
    for i in range(4): # handle a pillarperiod at a time
      queries = []
      queries.append( Transaction.t_date >= daybreaks[i] )
      queries.append( Transaction.t_date < daybreaks[i+1] )
      queries.append( Transaction.amount < 0 )
      queries.append( Transaction.item_id.in_(item_ids) )
      # count anything in Plaid category Transfer > Deposit as income
      queries.append( Transaction.category_uid.like("21007000") )
      # q = session.query(Transaction.category_uid, func.sum(Transaction.amount))
      q = session.query( func.sum(Transaction.amount) )
      q = q.filter(*queries)#.group_by(Transaction.category_uid).all()
      sumrec = q.first()
      # from sqlalchemy.dialects import postgresql
      # statement = q.statement
      # print("SQL: ")
      # print(statement.compile(dialect=postgresql.dialect()))
      inc = 0
      if sumrec[0] is not None:
        inc = -1 * int(sumrec[0]) # make the amount positive
        print('Income from %s to %s: %d' % (formatDate(daybreaks[i]), formatDate(daybreaks[i+1]), inc) )
      # put period's income into table
      periodincomes.append( inc )
      # save period's income to DB
      a = ActualMonthIncome( user_id=userid, start_date=daybreaks[i], amount=inc, period=i+1 )
      session.merge( a )

    allincomes.append( periodincomes )
    daybreaks = mkDayBreaks( daybreaks[4] )
    session.commit()

  # Compute average monthly income by period using allincomes list
  # (which is a 4 periods x 4 MONTHS_MEASURED list) and save to DB
  # DROP HIGHEST AND LOWEST MONTH AND AVERAGE THE OTHERS
  total_avg_inc = 0
  for i in range(MONTHS_MEASURED):
    periodincs = []
    for inc in allincomes:
      periodincs.append(inc[i])
    periodincs.sort()
    periodincs = periodincs[ 1:(len(periodincs)-1) ]

    periodinc = sum( periodincs )
    avg_inc = int( periodinc / (MONTHS_MEASURED-2) )
    a = AverageMonthIncome( user_id=userid, amount=avg_inc, period=i+1 )
    session.merge( a )
    total_avg_inc += avg_inc

  # Save average monthly income to user profile
  user = session.query(User).get(userid)
  user.income = int(total_avg_inc)
  user.income_update = datetime.datetime.today()
  session.commit()

  return True

def notice_is_pending(notices, notice):
  # check if this notice is in the user's "inbox"
  for n in notices:
    if n:
      if n['type'] == 'inspirational':
        if 'data' in n and n['data']['name'] == notice['name'] and n['data']['msg'] == notice['msg']:
          return True
      else:
        if n['msg'] == notice['msg'] and n['type'] == notice['type']:
          return True
  return False

def notice_has_seen(user, notice, session, days_since=14):
  # check if this notice is in the user's "inbox"
  is_pending = notice_is_pending(user.notices, notice)
  if is_pending:
    return True

  # now check if it has been archived in the last `days_since` days
  d = datetime.datetime.today()
  d = d - relativedelta(days=days_since)
  oldnotices = session.query(NoticeArchive).filter(NoticeArchive.user_id.like(user.id)).filter(NoticeArchive.created_on>d).all()
  for n in oldnotices:
    if n.notice['data']['name'] == notice['data']['name'] and n.notice['data']['msg'] == notice['data']['msg']:
      return True

  return False

def notice_has_rejected(user, notice, session, days_since=90):
  # Check if notice has been rejected in the last `days_since` days
  d = datetime.datetime.today()
  d = d - relativedelta(days=days_since)
  oldnotices = session.query(NoticeArchive).filter(NoticeArchive.user_id.like(user.id)).all()
  for n in oldnotices:
    if n.created_on > d:
      if n.notice['rejected']:
        if n.notice['data']['name'] == notice['name'] and n.notice['data']['msg'] == notice['msg']:
          return True

  return False

def notices_deferred(user, session):
  deferredn = []
  oldnotices = session.query(NoticeArchive).filter(NoticeArchive.user_id.like(user.id)).all()
  for n in oldnotices:
    if n.notice['deferred'] > 0 and n.notice['rejected'] == False:
      deferredn.append(n.notice)
  return deferredn  

def notice_debt_goal_behind_schedule(user):
  save_target = user.spending['save_target']
  inthehole = 0
  for goal in user.goals:
    goalattainmentmonths = 0

    if goal['amount'] != goal['progress']:
      attained = False
      goalattainmentmonths = 1
      neededsavings = goal['amount'] = goal['progress']
      savings = inthehole
      while not attained:
        savings += save_target
        if (savings - neededsavings) >= 0:
          inthehole -= neededsavings
          attained = True
        else:
          goalattainmentmonths = goalattainmentmonths + 1

    # see if months needed to attain goal match what the user has targeted
    if goal['type'] == 'debt':
      goaldate = mkDateTime(goal['due_date']) + relativedelta(months=1) # give wiggle room of 1 month
      ourprojectedgoaldate = datetime.datetime.today() + relativedelta(months=goalattainmentmonths)
      if ourprojectedgoaldate > goaldate:
        notice = {
        "msg": "Your debt-free goal will NOT happen at this savings level. Increase it on the Goals screen.", 
        "type": "warning", 
        "data":  None, 
        "acted": False, 
        "rejected": False, 
        "deferred": 0, 
        "priority": 8, 
        "timestamp": datetime.datetime.today().timestamp() * 1000
        }
        return notice
  return None
        
def notice_low_balance(user):
  accounts = user.balances
  if len(accounts) > 0:
    for account in accounts:
      if 'primary' in account and 'balances' in account:
        if account['primary'] == True and account['balances']['current'] < (user.income * 0.03):
          notice = {
          "msg": "Low primary account balance", 
          "type": "warning", 
          "data":  None, 
          "acted": False, 
          "rejected": False, 
          "deferred": 0, 
          "priority": 10, 
          "timestamp": datetime.datetime.today().timestamp() * 1000
          }
          return notice
  return None

def notice_back_to_school(user, session):
  if 'kids' in user.personal and int(user.personal['kids']) > 0:
    todaydt = datetime.datetime.today()
    if formatDate(todaydt,"%m") == '08':
      notice = {
      "msg": "Need to do some back-to-school shopping?", 
      "type": "goal", 
      "data":  SAMPLE_GOALS[0], 
      "acted": False, 
      "rejected": False, 
      "deferred": 0, 
      "priority": 5, 
      "timestamp": datetime.datetime.today().timestamp() * 1000
      }
      if not notice_has_seen(user, notice, session, 14) and not notice_has_rejected(user, notice, session):
        return notice
  return None

def notice_holiday():
  todaydt = datetime.datetime.today()
  d = formatDate(todaydt,"%m")
  if d == '10' or d == '03':
    notice = {
    "msg": "Last year, Americans spent about $967.13 on the holidays. Don't forget to save for it.", 
    "type": "goal", 
    "data":  SAMPLE_GOALS[1], 
    "acted": False, 
    "rejected": False, 
    "deferred": 0, 
    "priority": 6, 
    "timestamp": datetime.datetime.today().timestamp() * 1000
    }
    return notice
  return None

def notice_inspirational(user, session):
  todaydt = datetime.datetime.today()
  notice = {
    "msg": "An inspiring money management tip", 
    "type": "informational", 
    "data":  NOTICES_INSPIRE[random.randint(0, len(NOTICES_INSPIRE)-1)], 
    "acted": False, 
    "rejected": False, 
    "deferred": 0, 
    "priority": 1, 
    "timestamp": datetime.datetime.today().timestamp() * 1000
  }
  if not notice_has_seen(user, notice, session, 21):
    return notice
  return None

def do_notice(user, session):
  notices = []
  if user.notices and len(user.notices) > 0:
    notices = user.notices

  n = notices_deferred(user, session)
  if n and len(n)>0:
    notices += n

  n = notice_low_balance( user )
  if n:
    notices.append(n)

  n = notice_debt_goal_behind_schedule( user )
  if n and not notice_is_pending( notices, n ):
    notices.append(n)

  n = notice_holiday()
  if n:
    notices.append(n)

  n = notice_back_to_school( user, session )
  if n:
    notices.append(n)

  # default notices
  if len(notices) < 1:
    n = notice_inspirational( user, session)
    if n:
      notices.append(n)
  else:
    # sort by priority
    notices.sort( key=lambda x: x['priority'], reverse=True )

  user.notices = notices  
  user.notices_update = datetime.datetime.today()
  flag_modified(user, "notices")
  session.commit()

def expense_job():
  session = getsession()
  applog( {"msg":"starting job", "service":"aielf", "function":"expense_job"} , session )
  # userid = 'auth0|5b021d905d7d1617fd7dfadb'
  for user in session.query(User):
    userid = user.id
    success = compute_expenses(userid, session)
    success = compute_projected_spend(userid, session)
    success = projected_spend_to_budgets(userid, session)
  applog( {"msg":"success", "service":"aielf", "function":"expense_job"} , session )
  session.close()

def income_job():
  session = getsession()
  applog( {"msg":"starting job", "service":"aielf", "function":"income_job"} , session )
  # userid = 'auth0|5b021d905d7d1617fd7dfadb'
  for user in session.query(User):
    userid = user.id
    success = compute_income(userid, session)
  applog( {"msg":"success", "service":"aielf", "function":"income_job"} , session )
  session.close()

def notice_job():
  session = getsession()
  applog( {"msg":"starting job", "service":"aielf", "function":"income_job"}, session )
  for user in session.query(User):
    success = do_notice(user, session)
  applog( {"msg":"success", "service":"aielf", "function":"notice_job"}, session )
  session.close()

# expense_job()
# income_job()
# notice_job()

# schedule.every().day.at("11:35").do(expense_job)
# schedule.every().sunday.at("04:24").do(income_job)
schedule.every().day.at("03:35").do(notice_job)
schedule.every().day.at("21:21").do(expense_job)
schedule.every().day.at("02:21").do(income_job)


while True:
  schedule.run_pending()
  time.sleep(100)