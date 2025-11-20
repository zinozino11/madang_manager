import streamlit as st
import pandas as pd
import time
import duckdb
import os

# --- 데이터베이스 연결 및 초기화 함수 ---
def get_connection():
    # 1. DB 파일 연결 (없으면 생성됨)
    conn = duckdb.connect(database='madang.db', read_only=False)
    
    # 2. 테이블 존재 여부 확인
    try:
        # Book 테이블이 있는지 찔러봅니다.
        conn.execute("SELECT 1 FROM Book LIMIT 1")
    except duckdb.CatalogException:
        # 테이블이 없으면(에러나면) CSV 파일을 이용해 생성합니다.
        # 파일명이 정확해야 합니다 (이미지 기준: Book_madang.csv 등)
        
        # Book 테이블 생성
        if os.path.exists('Book_madang.csv'):
            conn.execute("CREATE TABLE OR REPLACE Book AS SELECT * FROM 'Book_madang.csv'")
        
        # Customer 테이블 생성
        if os.path.exists('Customer_madang.csv'):
            conn.execute("CREATE TABLE OR REPLACE Customer AS SELECT * FROM 'Customer_madang.csv'")
            
        # Orders 테이블 생성
        if os.path.exists('Orders_madang.csv'):
            conn.execute("CREATE TABLE OR REPLACE Orders AS SELECT * FROM 'Orders_madang.csv'")
            
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
    # Book 테이블에서 데이터 조회
    book_result = query("select concat(bookid, ',', bookname) from Book")
    for res in book_result:
        books.append(res[0]) # 튜플의 첫 번째 요소 추출
except Exception as e:
    st.error(f"데이터 초기화 중 오류 발생: {e}")
    st.stop()

tab1, tab2 = st.tabs(["고객조회", "거래 입력"])

# [탭 1] 고객 조회
with tab1:
    name_input = st.text_input("고객명 검색")
    if name_input:
        sql = f"""
            SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice 
            FROM Customer c, Book b, Orders o 
            WHERE c.custid = o.custid AND o.bookid = b.bookid AND c.name = '{name_input}'
        """
        result_df = query(sql, return_df=True)
        st.write(result_df)

# [탭 2] 거래 입력
with tab2:
    st.write("### 거래 입력")
    
    # 입력 폼
    input_custid = st.number_input("고객번호(custid)", value=1, step=1)
    select_book = st.selectbox("구매 서적:", books)
    input_price = st.text_input("금액")
    
    if st.button('거래 입력'):
        if select_book
