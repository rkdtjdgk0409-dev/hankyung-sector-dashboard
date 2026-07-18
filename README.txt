수정 파일
1. update_data.py -> 저장소 최상단의 기존 update_data.py를 교체
2. .github/workflows/main.yml -> 기존 워크플로 전체 교체

변경 핵심
- JSON 키의 밑줄/대소문자 정규화 오류 수정
- 등락률이 없는 한경 산업 매핑도 보존하고 KRX 등락률로 보완
- 한경 XHR/Fetch 응답의 도메인 및 Content-Type 제한 제거
- 코스피/코스닥 탭 스크롤, 더보기, 접힌 산업 항목 수집 강화
- 산업 ID와 산업명 분리 응답을 2단계로 연결
- 실패 시 data/diagnostics.json을 Actions Artifact로 업로드
