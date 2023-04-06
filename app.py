import psycopg2
from flask import Flask, jsonify, request
from con import set_connection
from loggerinstance import logger
import json

app = Flask(__name__)


def handle_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except psycopg2.Error as e:
            conn = kwargs.get('conn')
            if conn:
                conn.rollback()
            logger.error(str(e))
            return jsonify({"error": "Database error"})
        except Exception as e:
            logger.error(str(e))
            return jsonify({"error": "Internal server error"})
        finally:
            conn = kwargs.get('conn')
            cur = kwargs.get('cur')
            if cur:
                cur.close()
            if conn:
                conn.close()

    return wrapper


# tables

# CREATE TABLE stocks (
#     id SERIAL PRIMARY KEY,
#     name VARCHAR(50) NOT NULL,
#     quantity INTEGER NOT NULL,
#     price_per_unit FLOAT NOT NULL
# );
#
# CREATE TABLE transactions (
#     id SERIAL PRIMARY KEY,
#     stock_name VARCHAR(50) NOT NULL,
#     quantity INTEGER NOT NULL,
#     price_per_unit FLOAT NOT NULL,
#     transaction_type VARCHAR(10) NOT NULL,
#     transaction_date TIMESTAMP DEFAULT NOW()
# );


# Routes
@app.route('/v1/stock/buy', methods=['POST'])
@handle_exceptions
def buy_stock():
    # Extract data from request
    data = request.get_json()
    stock_name = data.get('stock_name')
    quantity = data.get('quantity')
    price_per_unit = data.get('price_per_unit')

    # Validate input data
    if not stock_name or not quantity or not price_per_unit:
        logger.error('Invalid input data')
        return jsonify({'message': 'Invalid input data'}), 400

    cur, conn = set_connection()

    # Insert data into database
    cur.execute("INSERT INTO stocks (name, quantity, price_per_unit) VALUES (%s, %s, %s);",
                (stock_name, quantity, price_per_unit))

    # Insert transaction data into database
    cur.execute("INSERT INTO transactions (stock_name, transaction_type, quantity, price_per_unit) "
                "VALUES (%s, 'buy', %s, %s);", (stock_name, quantity, price_per_unit))
    conn.commit()

    logger.info('Stock bought successfully')
    return jsonify({'message': 'Stock bought successfully'}), 201


@app.route('/v1/stock/sell', methods=['POST'], endpoint='sell_stock')
@handle_exceptions
def sell_stock():
    # Extract data from request
    data = request.get_json()
    stock_name = data.get('stock_name')
    quantity = data.get('quantity')
    price_per_unit = data.get('price_per_unit')

    # Validate input data
    if not stock_name or not quantity or not price_per_unit:
        logger.error('Invalid input data')
        return jsonify({'message': 'Invalid input data'}), 400

    cur, conn = set_connection()

    # Retrieve data from database
    cur.execute("SELECT quantity, price_per_unit FROM stocks WHERE name=%s;", (stock_name,))
    stock_data = cur.fetchone()

    # Check if stock exists in the database
    if not stock_data:
        logger.error('Stock not found in the database')
        return jsonify({'message': 'Stock not found in the database'}), 404

    # Check if the quantity is less than or equal to the available quantity
    if quantity > stock_data[0]:
        logger.error('Insufficient quantity')
        return jsonify({'message': 'Insufficient quantity'}), 400

    # Calculate profit or loss
    profit_or_loss = (price_per_unit - stock_data[1]) * quantity

    # Insert transaction data into database
    cur.execute("INSERT INTO transactions (stock_name, transaction_type, quantity, price_per_unit) "
                "VALUES (%s, 'sell', %s, %s);", (stock_name, quantity, price_per_unit))

    # Update data in database
    if quantity == stock_data[0]:
        cur.execute("DELETE FROM stocks WHERE name=%s;", (stock_name,))
    else:
        cur.execute("UPDATE stocks SET quantity=%s, price_per_unit=%s WHERE name=%s;",
                    (stock_data[0] - quantity, stock_data[1], stock_name))

    conn.commit()

    # Return success message with profit or loss
    if profit_or_loss > 0:
        message = f'Successfully sold {quantity} units of {stock_name} with a profit of {profit_or_loss}'
    elif profit_or_loss < 0:
        message = f'Successfully sold {quantity} units of {stock_name} with a loss of {profit_or_loss}'
    else:
        message = f'Successfully sold {quantity} units of {stock_name}'

    logger.info(message)
    return jsonify({'message': message}), 200


@app.route('/v1/stock/<string:name>', methods=['PUT'], endpoint='update_stock')
@handle_exceptions
def update_stock(name):
    # Extract data from request
    data = request.get_json()
    quantity = data.get('quantity')
    price_per_unit = data.get('price_per_unit')

    cur, conn = set_connection()
    # Retrieve data from database
    cur.execute("SELECT * FROM stocks WHERE name=%s;", (name,))
    stock_data = cur.fetchone()

    # Check if stock exists in the database
    if not stock_data:
        logger.warning(f'Stock {name} not found')
        return jsonify({'message': 'Stock not found'}), 404

    # Update data in database
    if quantity is not None:
        cur.execute("UPDATE stocks SET quantity=%s WHERE name=%s;", (quantity, name))
    if price_per_unit is not None:
        cur.execute("UPDATE stocks SET price_per_unit=%s WHERE name=%s;", (price_per_unit, name))
    conn.commit()

    logger.info(f'Stock {name} updated successfully')
    return jsonify({'message': 'Stock updated successfully'}), 200


@app.route('/v1/stock/<string:name>', methods=['DELETE'], endpoint='delete_stock')
@handle_exceptions
def delete_stock(name):
    cur, conn = set_connection()
    # Retrieve data from database
    cur.execute("SELECT * FROM stocks WHERE name=%s;", (name,))
    stock_data = cur.fetchone()

    # Check if stock exists in the database
    if not stock_data:
        logger.warning(f'Stock {name} not found')
        return jsonify({'message': 'Stock not found'}), 404

    # Delete data
    cur.execute("DELETE FROM stocks WHERE name=%s;", (name,))
    conn.commit()

    logger.info(f'Stock {name} deleted successfully')
    return jsonify({'message': 'Stock deleted successfully'}), 200


@app.route('/v1/stock/profit_loss', methods=['GET'], endpoint='calculate_profit_loss')
@handle_exceptions
def calculate_profit_loss():
    cur, conn = set_connection()
    # Retrieve data from database
    cur.execute("SELECT SUM(price_per_unit * quantity) FROM stocks;")
    total_value = cur.fetchone()[0]
    cur.execute("SELECT SUM(price_per_unit * quantity) FROM transactions WHERE transaction_type='sell';")
    total_sell_cost = cur.fetchone()[0]
    cur.execute("SELECT SUM(price_per_unit * quantity) FROM transactions WHERE transaction_type='buy';")
    total_buy_cost = cur.fetchone()[0]

    # Calculate profit or loss
    if total_sell_cost is None:
        total_sell_cost = 0
    if total_buy_cost is None:
        total_buy_cost = 0
    profit_or_loss = total_value - total_sell_cost + total_buy_cost
    # Log successful response
    logger.info(f"Profit/Loss calculation successful with result {profit_or_loss}")
    return jsonify({'profit_or_loss': profit_or_loss}), 200


if __name__ == "__main__":
    app.run(debug=True, port=5000)
