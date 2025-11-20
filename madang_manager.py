import streamlit as st
import pandas as pd
import time
import duckdb
import os

# --- 1. 데이터베이스 연결 설정 (캐싱 적용: 중요!) ---
# @st.cache_resource는 앱이 실행되는 동안 DB 연결을 끊지 않고 유지해줍니다.
@st.cache_resource
def get_connection():
    try:
        # DB 연결 (read_only=False 필수)
        conn = duckdb.connect(database='madang.db', read_only=False)
        
        # 테이블 초기화: CSV 파일이 있다면 테이블을 생성합니다.
        # (이미 테이블이 있어도 덮어쓰기 위해 CREATE OR REPLACE 사용)
        if os.path.exists('Book_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Book AS SELECT * FROM 'Book_madang.csv'")
        
        if os.path.exists('Customer_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Customer AS SELECT * FROM 'Customer_madang.csv'")
            
            # [자동 추가] 최진호 고객 데이터 (중복 방지 포함)
            conn.execute("""
                INSERT INTO Customer (custid, name, address, phone)
                SELECT 6, '최진호', '경기도', '010-7777-7777'
                WHERE NOT EXISTS (SELECT 1 FROM Customer WHERE custid = 6)
            """)
            
        if os.path.exists('Orders_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Orders AS SELECT * FROM 'Orders_madang.csv'")
        
        return conn
    except Exception as e:
        st.error(f"DB 연결/초기화 실패: {e}")
        return None

# 전역 변수로 연결 객체 받기
dbConn = get_connection()

# --- 2. 쿼리 실행 함수 ---
def run_query(sql, params=None):
    # 연결이 끊어졌을 경우를 대비해 커서를 매번 새로 생성
    if dbConn is None:
        return None
    
    cursor = dbConn.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        # SELECT문인 경우 결과를 반환, INSERT/UPDATE는 None 반환
        if sql.strip().upper().startswith('SELECT'):
            return cursor.df()
        else:
            return None # INSERT 등은 결과가 없으므로 None
    except Exception as e:
        st.warning(f"쿼리 실행 오류: {e}")
        return None

# --- 3. 메인 화면 UI ---
st.title("마당서점 관리 프로그램")

# 연결 실패 시 중단
if dbConn is None:
    st.stop()

# 책 목록 가져오기 (콤보박스용)
try:
    book_df = run_query("SELECT concat(bookid, ', ', bookname) as book_info FROM Book")
    if book_df is not None and not book_df.empty:
        book_list = book_df['book_info'].tolist()
    else:
        book_list = []
except:
    st.error("Book 테이블을 읽을 수 없습니다. CSV 파일을 확인해주세요.")
    st.stop()

# 탭 구성
tab1, tab2 = st.tabs([" 고객 조회", " 거래 입력"])

# --- [탭 1] 고객 조회 ---
with tab1:
    st.subheader("고객 구매 이력 조회")
    search_name = st.text_input("고객명 검색")
    
    if st.button("조회"):
        if search_name:
            sql = """
                SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice 
                FROM Customer c, Book b, Orders o 
                WHERE c.custid = o.custid AND o.bookid = b.bookid AND c.name = ?
            """
            result_df = run_query(sql, (search_name,))
            
            if result_df is not None and not result_df.empty:
                st.dataframe(result_df)
                total = result_df['saleprice'].sum()
                
            else:
                st.warning(f"'{search_name}' 고객의 구매 내역이 없습니다.")
        else:
            st.warning("검색할 이름을 입력하세요.")

# --- [탭 2] 거래 입력 ---
with tab2:
    st.subheader("신규 도서 판매 입력")
    
    # 입력 폼
    col1, col2 = st.columns(2)
    with col1:
        # 최진호(6번)을 기본값으로
        input_custid = st.number_input("고객번호 (ID)", value=6, min_value=1, step=1)
    with col2:
        input_price = st.number_input("판매 금액 (원)", value=13000, min_value=0, step=1000)
    
    select_book_str = st.selectbox("판매할 책 선택", book_list)
    
    if st.button("입력 완료", type="primary"):
        if select_book_str:
            # 책 ID 추출 (예: "1, 축구의 역사" -> 1)
            bookid = int(select_book_str.split(",")[0])
            
            # 날짜 (오늘)
            today = time.strftime('%Y-%m-%d', time.localtime())
            
            # 새 주문번호 생성 (MAX + 1)
            max_df = run_query("SELECT max(orderid) FROM Orders")
            current_max = max_df.iloc[0, 0] if (max_df is not None and not max_df.empty) else 0
            new_orderid = 1 if pd.isna(current_max) else int(current_max) + 1
            
            # INSERT 실행
            insert_sql = """
                INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) 
                VALUES (?, ?, ?, ?, ?)
            """
            # 실행
            run_query(insert_sql, (new_orderid, input_custid, bookid, input_price, today))
            
            st.success(f" 입력되었습니다! (주문번호: {new_orderid})")
            time.sleep(1)
            st.rerun() # 화면 새로고침
            
        else:
            st.error("책을 선택해주세요.")



