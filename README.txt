교체 파일
=========
1. update_data.py
2. requirements.txt
3. .github/workflows/main.yml

변경 사항
=========
- KRX 시장 전체 시가총액 API 사용 중단
- KODEX 200(069500) ETF 실제 구성종목 사용
- KODEX 코스닥150(229200) ETF 구성종목 중 비중 상위 100개 사용
- 종가, 등락률, 시가총액은 네이버 증권에서 종목별 별도 조회
- 업종은 한국경제 데이터센터 우선, 누락 종목만 네이버 금융 업종으로 보완

업로드 후
=========
Actions → 산업별 시장 대시보드 갱신 및 Pages 배포 → Run workflow
