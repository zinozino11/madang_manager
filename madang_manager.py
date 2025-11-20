import streamlit as st
import pandas as pd
import time
import duckdb
import os

# --- 데이터베이스 연결 및 초기화 함수 ---
def get_connection():
    # 1. DB 파일 연결
    conn = duckdb.connect(database='madang.db', read_only=False)
    
    # 2. 테이블 초기화 (CSV 파일이 있을 경우)
    # 문법 수정: 'CREATE TABLE OR REPLACE' (X) -> 'CREATE OR REPLACE TABLE' (O)
    try:
        # Book 테이블 생성
        if os.path.exists('Book_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Book AS SELECT * FROM 'Book_madang.csv'")
        
        # Customer 테이블 생성
        if os.path.exists('Customer_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Customer AS SELECT * FROM 'Customer_madang.csv'")
            
        # Orders 테이블 생성
        if os.path.exists('Orders_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Orders AS SELECT * FROM 'Orders_madang.csv'")
            
    except Exception as e:
        st.error(f"테이블 생성 중 오류가 발생했습니다: {e}")
            
    return conn

# DB 연결 객체 생성
dbConn = get_connection()
cursor = dbConn.cursor()

# --- 쿼리 실행 함수 ---
def query(sql, return_df=False):
    # 커서가 닫혀있을 경우를 대비해 커서를 다시 가져오는 로직을 추가할 수도 있습니다.
    # 여기서는 간단히 실행
    cursor.execute(sql)
    if return_df:
        return cursor.df()
    else:
        return cursor.fetchall()

# --- 메인 로직 ---

books = []
try:
    # Book 테이블 확인
    # 테이블이 제대로 생성되었는지 확인하는 쿼리
    cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'Book'")
    if cursor.fetchall()[0][0] == 0:
        st.error("데이터베이스에 Book 테이블이 없습니다. 'Book_madang.csv' 파일이 저장소에 있는지 확인해주세요.")
        st.stop()

    # 책 목록 조회
    book_result = query("select concat(bookid, ',', bookname) from Book")
    for res in book_result:
        books.append(res[0])
        
except Exception as e:
    st.error(f"데이터 조회 중 오류 발생: {e}")
    st.stop()

tab1, tab2 = st.tabs(["고객조회", "거래 입력"])

# [탭 1] 고객 조회
with tab1:
    name_input = st.text_input("고객명 검색")
    if name_input:
        # f-string 사용 시 SQL 인젝션 주의가 필요하지만, 학습용이므로 간단히 처리
        sql = f"""
            SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice 
            FROM Customer c, Book b, Orders o 
            WHERE c.custid = o.custid AND o.bookid = b.bookid AND c.name = '{name_input}'
        """
        try:
            result_df = query(sql, return_df=True)
            st.write(result_df)
        except Exception as e:
            st.error(f"조회 중 오류가 발생했습니다: {e}")

# [탭 2] 거래 입력
with tab2:
    st.write("### 거래 입력")
    
    input_custid = st.number_input("고객번호(custid)", value=1, step=1)
    select_book = st.selectbox("구매 서적:", books)
    input_price = st.text_input("금액")
    
    if st.button('거래 입력'):
        if select_book and input_price:
            try:
                bookid = select_book.split(",")[0]
                dt = time.strftime('%Y-%m-%d', time.localtime())
                
                # 주문번호 생성
                max_order_res = query("select max(orderid) from Orders")
                current_max = max_order_res[0][0]
                
                # 데이터가 없을 때(None) 처리
                if current_max is None:
                    new_orderid = 1
                else:
                    new_orderid = current_max + 1
                
                # 데이터 삽입
                insert_sql = f"""
                    INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) 
                    VALUES ({new_orderid}, {input_custid}, {bookid}, {input_price}, '{dt}')
                """
                cursor.execute(insert_sql)
                st.success(f'거래가 입력되었습니다. (주문번호: {new_orderid})')
                
            except Exception as e:
                st.error(f"입력 중 오류가 발생했습니다: {e}")
        else:
            st.warning("책과 금액을 모두 입력해주세요.")
