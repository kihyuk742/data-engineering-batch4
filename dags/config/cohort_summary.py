{
          'table': 'cohort_summary',
          'schema': 'analytics',
          'main_sql': """SELECT cohort_month, visited_month, cohort.userid
 FROM (
     SELECT userid, date_trunc('month', MIN(ts)) cohort_month
     FROM raw_data.user_session_channel usc
     JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
     GROUP BY 1
 ) cohort
 JOIN (
     SELECT DISTINCT userid, date_trunc('month', ts) visited_month
     FROM raw_data.user_session_channel usc
     JOIN raw_data.session_timestamp t ON t.sessionid = usc.sessionid
 ) visit ON cohort.cohort_month <= visit.visited_month and cohort.userid = visit.userid;
""",
          'input_check':
          [
            {
              'sql': 'SELECT COUNT(1) FROM raw_data.user_session_channel',
              'count': 101000
            },
          ],
          'output_check':
          [
            {
              'sql': 'SELECT COUNT(1) FROM {schema}.temp_{table}',
              'count': 4000
            }
          ],
}
