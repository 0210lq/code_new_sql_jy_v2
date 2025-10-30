import os
import sys
import pandas as pd
from datetime import datetime
import global_setting.global_dic as glv
import sys
import os
path = os.getenv('GLOBAL_TOOLSFUNC_new')
sys.path.append(path)
import global_tools as gt
from Optimizer_python.utils_log.logger import setup_logger, setup_check_logger
import yaml
from sqlalchemy import create_engine

# Setup loggers for this module
logger = setup_logger('data_check')
check_logger = setup_check_logger('data_check')

def config_path_finding():
    """Find the config directory path"""
    logger.info("Finding config path...")
    inputpath = os.path.split(os.path.realpath(__file__))[0]
    inputpath_output=None
    should_break=False
    for i in range(10):
        if should_break:
            break
        inputpath = os.path.dirname(inputpath)
        input_list = os.listdir(inputpath)
        for input in input_list:
            if should_break:
                break
            if str(input)=='config':
                inputpath_output=os.path.join(inputpath,input)
                should_break=True
    logger.info(f"Config path found: {inputpath_output}")
    return inputpath_output

def get_all_portfolio_names():
    """Get all portfolio names from mode dictionary"""
    logger.info("Getting all portfolio names from mode dictionary...")
    config_path = config_path_finding()
    inputpath = os.path.join(config_path, 'Score_config\\mode_dictionary.xlsx')
    df = pd.read_excel(inputpath)
    portfolio_names = df['score_name'].tolist()
    logger.info(f"Found {len(portfolio_names)} portfolio names")
    return portfolio_names

def get_db_connection():
    """Get database connection from config file"""
    current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    inputpath_configsql = os.path.join(current_dir, 'global_setting\\optimizer_sql.yaml')
    
    with open(inputpath_configsql, 'r') as f:
        config = yaml.safe_load(f)
    
    # Get Portfolio configuration
    portfolio_config = config.get('Portfolio', {})
    db_url = portfolio_config.get('db_url')
    
    if not db_url:
        raise ValueError("Missing database URL in configuration")
    
    # Parse the URL to get connection details
    # Format: mysql+pymysql://root:root@localhost:3306/portfolio?autocommit=True
    try:
        # Remove the mysql+pymysql:// prefix
        url_parts = db_url.replace('mysql+pymysql://', '')
        # Split into auth and rest
        auth, rest = url_parts.split('@')
        # Split auth into user and password
        user, password = auth.split(':')
        # Split rest into host_port and database
        host_port, database = rest.split('/')
        # Split host_port into host and port
        host, port = host_port.split(':')
        # Remove any query parameters from database
        database = database.split('?')[0]
        
        # Create connection string
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        logger.debug(f"Parsed database connection details from URL")
        
        # Create engine
        engine = create_engine(connection_string)
        return engine
        
    except Exception as e:
        raise ValueError(f"Failed to parse database URL: {str(e)}")

def check_sql_database(target_date):
    """
    Check if all portfolios have been saved to SQL database for the target date
    
    Args:
        target_date (str): Target date in YYYY-MM-DD format
    
    Returns:
        tuple: (missing_portfolios, error_portfolios)
    """
    logger.info(f"Checking SQL database for date: {target_date}")
    portfolio_names = get_all_portfolio_names()
    
    # Get database connection
    engine = get_db_connection()
    
    # First, get all portfolios for the target date
    query = f"""
    SELECT portfolio_name, COUNT(*) as stock_count, SUM(weight) as total_weight
    FROM Portfolio 
    WHERE valuation_date = '{target_date}'
    GROUP BY portfolio_name
    """
    logger.debug(f"Executing SQL query: {query}")
    
    # Execute query and get results using pandas
    df_db = pd.read_sql(query, engine)
    logger.debug(f"Query results: {df_db}")
    
    if df_db is None or df_db.empty:
        logger.error(f"No data found in database for date {target_date}")
        check_logger.error(f"No data found in database for date {target_date}")
        return portfolio_names, []
    
    # Convert to dictionary for easier lookup
    db_portfolios = df_db.set_index('portfolio_name').to_dict('index')
    
    missing_portfolios = []
    error_portfolios = []
    
    # Check each portfolio
    for portfolio in portfolio_names:
        if portfolio not in db_portfolios:
            missing_portfolios.append(portfolio)
            check_logger.warning(f"Portfolio {portfolio} not found in database for date {target_date}")
            continue
        
        # Get portfolio data
        portfolio_data = db_portfolios[portfolio]
        stock_count = portfolio_data['stock_count']
        total_weight = portfolio_data['total_weight']
        
        # Verify data
        if stock_count == 0:
            error_portfolios.append(portfolio)
            check_logger.error(f"Portfolio {portfolio} has no stocks in database for date {target_date}")
            continue
            
        if not (0.99 <= float(total_weight) <= 1.01):
            error_portfolios.append(portfolio)
            check_logger.error(f"Portfolio {portfolio} has invalid weight sum ({total_weight}) in database for date {target_date}")
            continue
        
        # Additional check: verify individual stock weights
        stock_query = f"""
        SELECT MIN(weight) as min_weight, MAX(weight) as max_weight
        FROM Portfolio 
        WHERE portfolio_name = '{portfolio}' 
        AND valuation_date = '{target_date}'
        """
        stock_df = pd.read_sql(stock_query, engine)
        
        if stock_df is not None and not stock_df.empty:
            min_weight = stock_df['min_weight'].iloc[0]
            max_weight = stock_df['max_weight'].iloc[0]
            
            # Check if weights are within acceptable range (-0.001 to 1)
            if float(min_weight) < -0.001 or float(max_weight) > 1:
                error_portfolios.append(portfolio)
                check_logger.error(f"Portfolio {portfolio} has invalid individual weights (min: {min_weight}, max: {max_weight})")
    
    logger.info(f"SQL database check completed. Missing: {len(missing_portfolios)}, Errors: {len(error_portfolios)}")
    check_logger.info(f"SQL database check completed. Missing: {len(missing_portfolios)}, Errors: {len(error_portfolios)}")
    
    # Log detailed results
    if missing_portfolios:
        check_logger.info(f"Missing portfolios: {', '.join(missing_portfolios)}")
    if error_portfolios:
        check_logger.info(f"Error portfolios: {', '.join(error_portfolios)}")
    
    return missing_portfolios, error_portfolios

def check_local_files(target_date):
    """
    Check if all portfolios have been updated for the target date in local files
    
    Args:
        target_date (str): Target date in YYYY-MM-DD format
    
    Returns:
        tuple: (missing_portfolios, error_portfolios)
    """
    logger.info(f"Checking local files for date: {target_date}")
    portfolio_names = get_all_portfolio_names()
    output_path = glv.get('output_weight')
    target_date2 = gt.intdate_transfer(target_date)
    
    missing_portfolios = []
    error_portfolios = []
    
    for portfolio in portfolio_names:
        try:
            file_path = os.path.join(output_path, portfolio, f"{portfolio}_{target_date2}.csv")
            if not os.path.exists(file_path):
                logger.warning(f"Missing local file for portfolio {portfolio} on {target_date}")
                check_logger.warning(f"Missing local file for portfolio {portfolio} on {target_date}")
                missing_portfolios.append(portfolio)
                continue
                
            # Verify file content
            df = pd.read_csv(file_path)
            if 'weight' not in df.columns or 'code' not in df.columns:
                logger.error(f"Invalid file format for portfolio {portfolio} on {target_date}")
                check_logger.error(f"Invalid file format for portfolio {portfolio} on {target_date}")
                error_portfolios.append(portfolio)
                continue
                
            # Check if weights sum to 1
            weight_sum = df['weight'].sum()
            if not (0.99 <= weight_sum <= 1.01):
                logger.error(f"Invalid weight sum ({weight_sum}) for portfolio {portfolio} on {target_date}")
                check_logger.error(f"Invalid weight sum ({weight_sum}) for portfolio {portfolio} on {target_date}")
                error_portfolios.append(portfolio)
                
        except Exception as e:
            logger.error(f"Error checking portfolio {portfolio}: {str(e)}")
            check_logger.error(f"Error checking portfolio {portfolio}: {str(e)}")
            error_portfolios.append(portfolio)
    
    logger.info(f"Local file check completed. Missing: {len(missing_portfolios)}, Errors: {len(error_portfolios)}")
    check_logger.info(f"Local file check completed. Missing: {len(missing_portfolios)}, Errors: {len(error_portfolios)}")
    return missing_portfolios, error_portfolios

def check_data_completeness(target_date=None):
    """
    Check data completeness for all portfolios
    
    Args:
        target_date (str, optional): Target date in YYYY-MM-DD format. If None, uses today's date.
    
    Returns:
        dict: Dictionary containing check results
    """
    if target_date is None:
        target_date = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Starting data completeness check for date: {target_date}")
    check_logger.info(f"Starting data completeness check for date: {target_date}")
    
    # Check local files
    local_missing, local_errors = check_local_files(target_date)
    
    # Check SQL database
    sql_missing, sql_errors = check_sql_database(target_date)
    
    # Prepare results
    results = {
        'target_date': target_date,
        'local_files': {
            'missing': local_missing,
            'errors': local_errors
        },
        'sql_database': {
            'missing': sql_missing,
            'errors': sql_errors
        }
    }
    
    # Log summary
    summary = f"""
Data Completeness Check Summary for {target_date}:
=============================================
Local Files:
- Missing Portfolios: {len(local_missing)}
- Error Portfolios: {len(local_errors)}
- Missing List: {', '.join(local_missing) if local_missing else 'None'}
- Error List: {', '.join(local_errors) if local_errors else 'None'}

SQL Database:
- Missing Portfolios: {len(sql_missing)}
- Error Portfolios: {len(sql_errors)}
- Missing List: {', '.join(sql_missing) if sql_missing else 'None'}
- Error List: {', '.join(sql_errors) if sql_errors else 'None'}
=============================================
"""
    logger.info("Data completeness check completed")
    check_logger.info(summary)
    
    return results

if __name__ == '__main__':
    # Example usage
    results = check_data_completeness('2025-06-03')
    print("\nCheck Results:")
    print(f"Target Date: {results['target_date']}")
    print("\nLocal Files:")
    print(f"Missing: {results['local_files']['missing']}")
    print(f"Errors: {results['local_files']['errors']}")
    print("\nSQL Database:")
    print(f"Missing: {results['sql_database']['missing']}")
    print(f"Errors: {results['sql_database']['errors']}") 