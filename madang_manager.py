import streamlit as st
import pandas as pd
import time
import duckdb
import os

# --- 1. DB 연결 설정 (연결 유지 기능 포함) ---
@st.cache_resource
def get_connection():
    try:
        conn = duckdb.connect(database='madang.db', read_only=False)
        
        # 테이블이 없으면 CSV에서 생성
        if os.path.exists('Book_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Book AS SELECT * FROM 'Book_madang.csv'")
        if os.path.exists('Customer_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Customer AS SELECT * FROM 'Customer_madang.csv'")
            # 최진호 데이터 자동 추가 (안전장치)
            conn.execute("INSERT INTO Customer (custid, name, address, phone) SELECT 6, '최진호', '경기도', '010-7777-7777' WHERE NOT EXISTS (SELECT 1 FROM Customer WHERE custid = 6)")
        if os.path.exists('Orders_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Orders AS SELECT * FROM 'Orders_madang.csv'")
        
        return conn
    except Exception as e:
        st.error(f"DB 연결 오류: {e}")
        return None

dbConn = get_connection()

# --- 2. 쿼리 실행 함수 ---
def run_query(sql, params=None):
    if dbConn is None: return None
    cursor = dbConn.cursor()
    try:
        if params: cursor.execute(sql, params)
        else: cursor.execute(sql)
        
        if sql.strip().upper().startswith('SELECT'): return cursor.df()
        return None
    except Exception as e:
        st.warning(f"실행 오류: {e}")
        return None

# --- 3. 세션 상태 초기화 (탭 간 데이터 공유용) ---
if 'selected_custid' not in st.session_state:
    st.session_state['selected_custid'] = 6  # 기본값: 최진호
if 'selected_name' not in st.session_state:
    st.session_state['selected_name'] = '최진호'

# --- 4. 메인 로직 ---

# 책 목록 가져오기
books = []
try:
    book_df = run_query("SELECT concat(bookid, ',', bookname) FROM Book")
    if book_df is not None and not book_df.empty:
        books = book_df.iloc[:, 0].tolist()
except:
    pass

tab1, tab2 = st.tabs(["고객조회", "거래 입력"])

# [탭 1] 고객 조회
with tab1:
    name = st.text_input("고객명")
    
    if name: # 이름이 입력되면 실행
        sql = "select c.custid, c.name, b.bookname, o.orderdate, o.saleprice from Customer c, Book b, Orders o where c.custid = o.custid and o.bookid = b.bookid and name = ?"
        result = run_query(sql, (name,))
        
        if result is not None and not result.empty:
            st.write(result)
            # 검색된 고객 정보를 세션에 저장 (탭 2로 넘기기 위해)
            st.session_state['selected_custid'] = result.iloc[0]['custid']
            st.session_state['selected_name'] = result.iloc[0]['name']
        else:
            st.warning("구매 내역이 없는 고객입니다.")
            # 구매 내역은 없어도 고객 정보가 있는지 확인하여 ID 가져오기
            cust_info = run_query("SELECT custid, name FROM Customer WHERE name = ?", (name,))
            if cust_info is not None and not cust_info.empty:
                st.session_state['selected_custid'] = cust_info.iloc[0]['custid']
                st.session_state['selected_name'] = cust_info.iloc[0]['name']

# [탭 2] 거래 입력 (원래 형식으로 복구)
with tab2:
    # 1. 원래 코드처럼 텍스트로 정보 표시
    custid = st.session_state['selected_custid']
    name_val = st.session_state['selected_name']
    
    st.write("고객번호: " + str(custid))
    st.write("고객명: " + name_val)
    
    # 2. 원래 코드의 입력 스타일 유지
    select_book = st.selectbox("구매 서적:", books)
    price = st.text_input("금액") # text_input으로 복구
    
    if st.button('거래 입력'):
        if select_book and price:
            try:
                # 데이터 전처리
                bookid = select_book.split(",")[0]
                dt = time.strftime('%Y-%m-%d', time.localtime())
                
                # 금액이 문자인 경우 숫자로 변환 (에러 방지)
                if not price.isdigit():
                    st.error("금액은 숫자만 입력해주세요.")
                    st.stop()
                
                # 주문번호 생성
                max_res = run_query("select max(orderid) from orders")
                current_max = max_res.iloc[0, 0] if max_res is not None else 0
                new_orderid = 1 if pd.isna(current_max) else int(current_max) + 1
                
                # INSERT 실행
                sql = "insert into orders (orderid, custid, bookid, saleprice, orderdate) values (?, ?, ?, ?, ?)"
                run_query(sql, (new_orderid, custid, bookid, price, dt))
                
                st.write('거래가 입력되었습니다.') # 원래 메시지 스타일
                time.sleep(1)
                st.rerun() # 입력 후 갱신
                
            except Exception as e:
                st.error(f"입력 에러: {e}")
        else:
            st.write("책과 금액을 입력하세요.")
