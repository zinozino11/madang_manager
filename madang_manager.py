import streamlit as st
import pandas as pd
import time
import duckdb
import os

# 1. DB 연결 및 테이블 초기화 함수
def get_connection():
    conn = duckdb.connect(database='madang.db', read_only=False)
    
    # 테이블이 없는 경우 CSV에서 로드 (최초 1회 실행용 안전장치)
    # 주의: 같은 폴더에 csv 파일들이 있어야 합니다. 파일명이 다르다면 수정해주세요.
    try:
        # Book 테이블 확인 및 생성
        conn.execute("SELECT count(*) FROM Book")
    except:
        # 테이블이 없으면 CSV에서 생성 (CSV 파일명이 정확해야 함)
        # 만약 CSV가 없다면 이 부분에서 에러가 날 수 있으니, 
        # 로컬에서 미리 madang.db를 만들어서 올리는 것이 가장 좋습니다.
        if os.path.exists('Book.csv'): 
            conn.execute("CREATE TABLE IF NOT EXISTS Book AS SELECT * FROM 'Book.csv'")
        if os.path.exists('Customer.csv'):
            conn.execute("CREATE TABLE IF NOT EXISTS Customer AS SELECT * FROM 'Customer.csv'")
        if os.path.exists('Orders.csv'):
            conn.execute("CREATE TABLE IF NOT EXISTS Orders AS SELECT * FROM 'Orders.csv'")
            
    return conn

dbConn = get_connection()
cursor = dbConn.cursor()

# 2. 쿼리 실행 함수 (DataFrame 반환 모드 추가)
def query(sql, return_df=False):
    cursor.execute(sql)
    if return_df:
        return cursor.df() # DataFrame으로 반환 (컬럼명 유지)
    else:
        return cursor.fetchall() # 리스트+튜플로 반환

# --- 메인 로직 시작 ---

books = [None]
# DuckDB의 concat 함수 사용
# fetchall 결과는 [(1, '축구의 역사'), ...] 형태임
try:
    book_result = query("select concat(bookid, ',', bookname) from Book")
    for res in book_result:
        # res는 튜플이므로 res[0]으로 접근해야 함 (res.values() 사용 불가)
        books.append(res[0])
except duckdb.CatalogException:
    st.error("데이터베이스에 테이블이 없습니다. CSV 파일을 확인하거나 madang.db를 업로드하세요.")
    st.stop()

tab1, tab2 = st.tabs(["고객조회", "거래 입력"])

# --- 탭 1: 고객 조회 ---
with tab1:
    name = st.text_input("고객명 검색") # tab1.text_input -> st.text_input 권장
    if name:
        # SQL에서 문자열은 작은따옴표('')로 감싸야 함
        sql = f"""
            SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice 
            FROM Customer c, Book b, Orders o 
            WHERE c.custid = o.custid AND o.bookid = b.bookid AND c.name = '{name}'
        """
        # 결과를 바로 DataFrame으로 받음 (컬럼명 문제 해결)
        result_df = query(sql, return_df=True)
        
        st.write(result_df)
        
        if not result_df.empty:
            # 전역 변수 대신 session_state나 직접 값 사용
            selected_custid = result_df.iloc[0]['custid']
            selected_name = name
        else:
            st.warning("구매 내역이 없는 고객입니다.")

# --- 탭 2: 거래 입력 ---
with tab2:
    # 탭1에서 검색한 내용이 있을 때만 표시하고 싶다면 로직 연결이 필요하지만,
    # 일단 독립적으로 입력받도록 구성
    
    st.write("### 거래 입력")
    # 입력 폼
    input_custid = st.number_input("고객번호(custid)", value=999) 
    # (위에서 검색된 custid를 자동으로 넣으려면 session_state를 써야 하는데 일단 수동 입력으로 둠)
    
    select_book = st.selectbox("구매 서적:", books)
    input_price = st.text_input("금액")
    
    if st.button('거래 입력'):
        if select_book and input_price:
            bookid = select_book.split(",")[0]
            
            # 날짜 구하기
            dt = time.strftime('%Y-%m-%d', time.localtime())
            
            # 주문번호 생성 (최대값 + 1)
            # fetchall 결과는 [(10,)] 형태 -> [0][0]으로 접근
            max_order_res = query("select max(orderid) from orders")
            if max_order_res[0][0] is None:
                new_orderid = 1
            else:
                new_orderid = max_order_res[0][0] + 1
            
            # INSERT 실행
            insert_sql = f"""
                INSERT INTO orders (orderid, custid, bookid, saleprice, orderdate) 
                VALUES ({new_orderid}, {input_custid}, {bookid}, {input_price}, '{dt}')
            """
            cursor.execute(insert_sql)
            # dbConn.commit() # DuckDB는 기본적으로 자동 커밋되지만 명시해도 됨
            
            st.success(f'거래가 입력되었습니다. (주문번호: {new_orderid})')
        else:
            st.error("책과 금액을 모두 입력해주세요.")
