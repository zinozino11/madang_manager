import streamlit as st
import pandas as pd
import time
import duckdb
import os

# --- 1. 데이터베이스 연결 및 초기화 ---
def get_connection():
    conn = duckdb.connect(database='madang.db', read_only=False)
    
    try:
        # Book 테이블 생성 및 데이터 로드
        if os.path.exists('Book_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Book AS SELECT * FROM 'Book_madang.csv'")
        
        # Customer 테이블 생성 및 데이터 로드
        if os.path.exists('Customer_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Customer AS SELECT * FROM 'Customer_madang.csv'")
            
            # [자동 추가] 최진호 고객 데이터 (없을 경우에만 추가)
            conn.execute("""
                INSERT INTO Customer (custid, name, address, phone)
                SELECT 6, '최진호', '경기도', '010-7777-7777'
                WHERE NOT EXISTS (SELECT 1 FROM Customer WHERE custid = 6)
            """)
            
        # Orders 테이블 생성 및 데이터 로드
        if os.path.exists('Orders_madang.csv'):
            conn.execute("CREATE OR REPLACE TABLE Orders AS SELECT * FROM 'Orders_madang.csv'")
            
    except Exception as e:
        st.error(f"DB 초기화 중 오류: {e}")
            
    return conn

# 연결 객체 생성
dbConn = get_connection()
cursor = dbConn.cursor()

# --- 2. 쿼리 실행 함수 ---
def query(sql, params=None, return_df=False):
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
        
    if return_df:
        return cursor.df()
    else:
        return cursor.fetchall()

# --- 3. 메인 로직 ---
st.title("마당서점 관리자")

# 책 목록 가져오기 (콤보박스용)
books = []
try:
    # 테이블 존재 확인
    check_table = cursor.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'Book'").fetchall()
    if check_table[0][0] == 0:
        st.error("Book 테이블이 없습니다. CSV 파일을 확인하세요.")
        st.stop()

    book_result = query("SELECT concat(bookid, ', ', bookname) FROM Book")
    for res in book_result:
        books.append(res[0])
except Exception as e:
    st.error(f"초기 데이터 로드 실패: {e}")
    st.stop()

tab1, tab2 = st.tabs(["고객조회", "거래 입력"])

# --- [탭 1] 고객 조회 ---
with tab1:
    st.header("고객별 구매 내역")
    name_input = st.text_input("고객명 검색", placeholder="예: 최진호")
    
    if name_input:
        sql = """
            SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice 
            FROM Customer c, Book b, Orders o 
            WHERE c.custid = o.custid AND o.bookid = b.bookid AND c.name = ?
        """
        try:
            result_df = query(sql, params=(name_input,), return_df=True)
            
            if not result_df.empty:
                st.dataframe(result_df)
                # 총 구매액 계산
                total_price = result_df['saleprice'].sum()
                st.success(f"총 구매액: {total_price:,}원")
            else:
                st.warning(f"'{name_input}' 고객의 구매 내역이 없습니다.")
        except Exception as e:
            st.error(f"조회 에러: {e}")

# --- [탭 2] 거래 입력 (수정된 부분) ---
with tab2:
    st.header("신규 거래 입력")
    
    # 1. 입력 폼 개선
    col1, col2 = st.columns(2)
    with col1:
        # 최진호(6번) 기본값
        input_custid = st.number_input("고객번호(custid)", value=6, step=1, min_value=1)
    with col2:
        # 금액을 숫자 입력칸으로 변경 (중요!)
        input_price = st.number_input("판매 금액", min_value=0, step=1000, value=10000)

    select_book = st.selectbox("구매 서적 선택", books)
    
    if st.button('거래 입력', type="primary"):
        if select_book:
            try:
                # 데이터 준비
                bookid = int(select_book.split(",")[0]) # 콤마 앞의 숫자만 추출
                dt = time.strftime('%Y-%m-%d', time.localtime())
                
                # 주문번호 생성 logic
                max_res = query("SELECT max(orderid) FROM Orders")
                current_max = max_res[0][0]
                new_orderid = 1 if current_max is None else current_max + 1
                
                # INSERT 실행 (파라미터 바인딩 사용으로 안전성 확보)
                insert_sql = """
                    INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) 
                    VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(insert_sql, (new_orderid, input_custid, bookid, input_price, dt))
                
                # 커밋 (DuckDB는 자동 커밋되지만 확실하게 처리)
                # dbConn.commit() 
                
                st.success(f"✅ 거래 입력 완료! (주문번호: {new_orderid})")
                time.sleep(1) # 메시지를 보여줄 시간 확보
                st.rerun() # 화면 새로고침 (입력 결과 즉시 반영을 위해)
                
            except Exception as e:
                st.error(f"❌ 입력 실패: {e}")
        else:
            st.warning("책을 선택해주세요.")
