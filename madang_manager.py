import streamlit as st
import pandas as pd
import time
import duckdb
import os

# --- 데이터베이스 연결 및 초기화 함수 ---
def get_connection():
    # 1. DB 파일 연결
    conn = duckdb.connect(database='madang.db', read_only=False)
    
    # 2. 테이블 초기화 (CSV 파일 기반)
    try:
        # Book 테이블 생성
        if os.path.exists('Book_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Book AS SELECT * FROM 'Book_madang.csv'")
        
        # Customer 테이블 생성
        if os.path.exists('Customer_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Customer AS SELECT * FROM 'Customer_madang.csv'")
            
            # [추가된 코드] 요청하신 '최진호' 고객 데이터 추가
            # 테이블이 초기화될 때마다 이 데이터가 삽입됩니다.
            # 혹시 CSV에 이미 6번이 있다면 중복될 수 있으니 체크 후 삽입 (안전장치)
            conn.execute("""
                INSERT INTO Customer (custid, name, address, phone)
                SELECT 6, '최진호', '경기도', '010-7777-7777'
                WHERE NOT EXISTS (SELECT 1 FROM Customer WHERE custid = 6)
            """)
            
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
    cursor.execute(sql)
    if return_df:
        return cursor.df()
    else:
        return cursor.fetchall()

# --- 메인 로직 ---

books = []
try:
    # 테이블 존재 여부 확인 (안전장치)
    cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'Book'")
    if cursor.fetchall()[0][0] == 0:
        st.error("데이터베이스 테이블이 없습니다. CSV 파일(Book_madang.csv 등)을 확인해주세요.")
        st.stop()

    # 책 목록 조회 (콤보박스용)
    book_result = query("select concat(bookid, ',', bookname) from Book")
    for res in book_result:
        books.append(res[0])
        
except Exception as e:
    st.error(f"데이터 조회 중 오류 발생: {e}")
    st.stop()

tab1, tab2 = st.tabs(["고객조회", "거래 입력"])

# [탭 1] 고객 조회
with tab1:
    st.write("### 고객별 구매 내역 조회")
    name_input = st.text_input("고객명 검색")
    
    if name_input:
        sql = f"""
            SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice 
            FROM Customer c, Book b, Orders o 
            WHERE c.custid = o.custid AND o.bookid = b.bookid AND c.name = '{name_input}'
        """
        try:
            result_df = query(sql, return_df=True)
            
            if not result_df.empty:
                st.dataframe(result_df)
            else:
                # 고객 정보만이라도 있는지 확인해서 보여주면 더 친절함
                check_cust = query(f"SELECT * FROM Customer WHERE name = '{name_input}'", return_df=True)
                if not check_cust.empty:
                    
                    st.dataframe(check_cust)

        except Exception as e:
            st.error(f"조회 중 오류가 발생했습니다: {e}")

# [탭 2] 거래 입력
with tab2:
    st.write("### 신규 거래 입력")
    
    # 최진호(6번)을 기본값으로 설정하기 위해 value=6으로 변경
    input_custid = st.number_input("고객번호(custid)", value=6, step=1)
    select_book = st.selectbox("구매 서적:", books)
    input_price = st.text_input("금액")
    
    if st.button('거래 입력'):
        if select_book and input_price:
            try:
                bookid = select_book.split(",")[0]
                dt = time.strftime('%Y-%m-%d', time.localtime())
                
                # 주문번호 생성 (최대값 + 1)
                max_order_res = query("select max(orderid) from Orders")
                current_max = max_order_res[0][0]
                
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
               
                
            except Exception as e:
                st.error(f"입력 중 오류가 발생했습니다: {e}")
        else:
            st.warning("책과 금액을 모두 입력해주세요.")
    

