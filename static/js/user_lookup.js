let searchTimeout;
let selectedIndex = -1; // 키보드로 선택된 항목 인덱스
let lastSearchedUsername = ''; // 마지막으로 검색한 유저 이름 저장
const searchInput = document.getElementById('userSearchInput');
const autocompleteDropdown = document.getElementById('autocompleteDropdown');
const searchButton = document.getElementById('searchButton');
const searchResults = document.getElementById('searchResults');

// 자동완성
searchInput.addEventListener('input', function() {
    clearTimeout(searchTimeout);
    let query = this.value.trim();
    selectedIndex = -1; // 검색어 변경 시 선택 초기화
    
    // @ prefix 제거
    if (query.startsWith('@')) {
        query = query.substring(1).trim();
        this.value = query;
    }
    
    // 최소 1글자 이상 (한글 1글자도 포함)
    if (query.length < 1) {
        autocompleteDropdown.style.display = 'none';
        return;
    }
    
    searchTimeout = setTimeout(() => {
        const encodedQuery = encodeURIComponent(query);
        
        fetch(`/api/user-search?q=${encodedQuery}`)
            .then(res => {
                if (!res.ok) {
                    throw new Error('Network response was not ok');
                }
                return res.json();
            })
            .then(data => {
                if (!data || data.length === 0) {
                    autocompleteDropdown.style.display = 'none';
                    return;
                }
                
                autocompleteDropdown.innerHTML = data.map(user => {
                    // Kaito 이미지 ID 감지 (숫자만 있는 경우)
                    let imageUrl = user.imageUrl;
                    if (imageUrl && /^\d+$/.test(imageUrl)) {
                        // 숫자만 있으면 Kaito 이미지 ID로 간주하고 서버 프록시 사용
                        imageUrl = `/kaito-img/${imageUrl}`;
                    }
                    
                    return `
                        <div class="autocomplete-item" data-username="${user.infoName}">
                            ${imageUrl ? `<img src="${imageUrl}" alt="${user.displayName || user.infoName}" onerror="this.style.display='none'">` : ''}
                            <div>
                                <strong>${user.displayName || user.infoName}</strong>
                                <div class="text-muted">@${user.infoName}</div>
                            </div>
                        </div>
                    `;
                }).join('');
                
                autocompleteDropdown.style.display = 'block';
                
                // 클릭 이벤트
                document.querySelectorAll('.autocomplete-item').forEach(item => {
                    item.addEventListener('click', function() {
                        const username = this.dataset.username;
                        searchInput.value = username;
                        autocompleteDropdown.style.display = 'none';
                        loadUserData(username);
                    });
                });
            })
            .catch(err => {
                autocompleteDropdown.style.display = 'none';
            });
    }, 300);
});

// 키보드 방향키 및 엔터 처리
searchInput.addEventListener('keydown', function(e) {
    const items = autocompleteDropdown.querySelectorAll('.autocomplete-item');
    
    if (items.length === 0) return;
    
    if (e.key === 'ArrowDown') {
        e.preventDefault();
        selectedIndex = (selectedIndex + 1) % items.length;
        updateSelection(items);
    } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        selectedIndex = (selectedIndex - 1 + items.length) % items.length;
        updateSelection(items);
    } else if (e.key === 'Enter') {
        e.preventDefault();
        if (selectedIndex >= 0 && selectedIndex < items.length) {
            // 선택된 항목이 있으면 해당 유저 로드
            const username = items[selectedIndex].dataset.username;
            searchInput.value = username;
            autocompleteDropdown.style.display = 'none';
            loadUserData(username);
        } else {
            // 선택된 항목이 없으면 입력값으로 검색
            let username = this.value.trim();
            // @ prefix 제거
            if (username.startsWith('@')) {
                username = username.substring(1).trim();
            }
            if (username) {
                autocompleteDropdown.style.display = 'none';
                loadUserData(username);
            }
        }
    } else if (e.key === 'Escape') {
        autocompleteDropdown.style.display = 'none';
        selectedIndex = -1;
    }
});

// 선택된 항목 업데이트 (하이라이트)
function updateSelection(items) {
    items.forEach((item, index) => {
        if (index === selectedIndex) {
            item.classList.add('active');
            item.scrollIntoView({ block: 'nearest' });
        } else {
            item.classList.remove('active');
        }
    });
}

// 외부 클릭 시 드롭다운 닫기 및 검색창 값 복원
document.addEventListener('click', function(e) {
    if (!searchInput.contains(e.target) && !autocompleteDropdown.contains(e.target)) {
        autocompleteDropdown.style.display = 'none';
        selectedIndex = -1;
        
        // 검색창이 비어있고 마지막 검색어가 있으면 복원
        if (!searchInput.value.trim() && lastSearchedUsername) {
            searchInput.value = lastSearchedUsername;
        }
    }
});

// 검색창 클릭 시 내용 지우기
searchInput.addEventListener('focus', function() {
    // 검색 결과가 표시된 상태에서만 클릭 시 지우기
    if (searchResults.innerHTML && !searchResults.innerHTML.includes('spinner-border')) {
        this.value = '';
    }
});

// 검색 버튼 클릭
searchButton.addEventListener('click', function() {
    let username = searchInput.value.trim();
    
    // 입력값이 없고 마지막 검색어가 있으면 복원
    if (!username && lastSearchedUsername) {
        searchInput.value = lastSearchedUsername;
        return;
    }
    
    // @ prefix 제거
    if (username.startsWith('@')) {
        username = username.substring(1).trim();
    }
    if (username) {
        loadUserData(username);
    }
});

// 사용자 데이터 로드
function loadUserData(username) {
    searchResults.innerHTML = '<div class="text-center"><div class="spinner-border" role="status"></div></div>';
    
    // 검색창에 검색한 유저 이름 표시 및 저장
    searchInput.value = username;
    lastSearchedUsername = username; // 마지막 검색어 저장
    
    // URL에 username 추가
    const url = new URL(window.location);
    url.searchParams.set('username', username);
    if (window.location.search !== url.search) {
        window.history.pushState({}, '', url);
    }
    
    // 사용자 데이터와 YAPS 데이터를 병렬로 가져오기 (서버 프록시 사용)
    Promise.all([
        fetch(`/api/user-data/${encodeURIComponent(username)}`).then(res => res.json()),
        fetch(`/api/yaps/${encodeURIComponent(username)}`)
            .then(res => res.json())
            .catch(() => null) // YAPS API 실패 시 무시
    ])
        .then(([userData, yapsData]) => {
            if (!userData || userData.error) {
                searchResults.innerHTML = `
                    <div class="alert alert-warning text-center">
                        <i class="fas fa-exclamation-triangle"></i> 사용자의 정보가 존재하지 않습니다
                    </div>
                `;
                return;
            }
            
            // YAPS 데이터를 사용자 데이터에 추가
            if (yapsData && !yapsData.error) {
                userData.yaps = yapsData;
            }
            
            renderUserData(userData);
        })
        .catch(err => {
            searchResults.innerHTML = `
                <div class="alert alert-danger text-center">
                    <i class="fas fa-times-circle"></i> 오류가 발생했습니다
                </div>
            `;
        });
}

// timeframe 정렬 함수
function sortTimeframes(rankings) {
    // 기본 우선순위 (알려진 패턴)
    const knownOrder = {
        '7D': 1, '7d': 1, 
        '14D': 2, '14d': 2, 
        '30D': 3, '30d': 3,
        '90D': 4, '90d': 4,
        '180D': 5, '180d': 5,
        '360D': 6, '360d': 6,
        'TOTAL': 7, 'total': 7
    };
    
    return rankings.sort((a, b) => {
        const tfA = a.timeframe;
        const tfB = b.timeframe;
        
        // 알려진 순서가 있으면 사용
        const aOrder = knownOrder[tfA];
        const bOrder = knownOrder[tfB];
        
        if (aOrder !== undefined && bOrder !== undefined) {
            return aOrder - bOrder;
        }
        if (aOrder !== undefined) return -1;  // A가 알려진 순서면 먼저
        if (bOrder !== undefined) return 1;   // B가 알려진 순서면 먼저
        
        // 둘 다 알려지지 않은 경우: epoch 패턴 처리
        const isEpochA = tfA.toLowerCase().startsWith('epoch');
        const isEpochB = tfB.toLowerCase().startsWith('epoch');
        
        if (isEpochA && isEpochB) {
            // 둘 다 epoch인 경우 문자열 비교 (epoch-2, epoch-omega 등)
            return tfA.localeCompare(tfB);
        }
        if (isEpochA) return 1;  // epoch는 뒤로
        if (isEpochB) return -1; // epoch는 뒤로
        
        // 나머지는 알파벳 순
        return tfA.localeCompare(tfB);
    });
}

// Cookie 프로젝트명 포맷 함수 (언어 플래그 추가 및 언어 코드 제거)
function formatCookieProjectName(projectName, suffix) {
    const flags = {'ko': '🇰🇷', 'en': '🌐', 'zh': '🇨🇳', 'pt': '🇵🇹', 'es': '🇪🇸'};
    const upperName = projectName.toUpperCase();
    
    // 언어 코드 패턴 확인
    const langMatch = upperName.match(/-(EN|KO|PT|ES|ZH)$/);
    if (langMatch) {
        const lang = langMatch[1].toLowerCase();
        const baseName = upperName.substring(0, upperName.length - 3); // -XX 제거
        const flag = flags[lang] || '🌐';
        return `<span class="flag-emoji">${flag}</span><span>${baseName} ${suffix}</span>`;
    }
    return `<span>${upperName} ${suffix}</span>`;
}

// 사용자 데이터 렌더링
function renderUserData(data) {
    const user = data.user;
    
    // 통계 데이터 배열 생성 (그룹화된 형태로)
    const stats = [];
    
    // 1. 팔로워 (단일 항목)
    if (user.follower) {
        stats.push(`
            <div class="stat-group">
                <small class="text-muted d-block mb-1">Followers</small>
                <strong class="d-block">${user.follower.toLocaleString()}</strong>
            </div>
        `);
    }
    
    // 2. 스마트 팔로워 (그룹)
    if (user.kaito_smart_follower || user.cookie_smart_follower) {
        let smartFollowerItems = [];
        if (user.kaito_smart_follower) {
            smartFollowerItems.push(`<div class="stat-item"><span class="stat-label"><img src="/static/kaito.png" alt="Kaito" style="width: 16px; height: 16px; margin-right: 4px;">Kaito</span><strong>${user.kaito_smart_follower.toLocaleString()}</strong></div>`);
        }
        if (user.cookie_smart_follower) {
            smartFollowerItems.push(`<div class="stat-item"><span class="stat-label">🍪 Cookie</span><strong>${user.cookie_smart_follower.toLocaleString()}</strong></div>`);
        }
        
        stats.push(`
            <div class="stat-group">
                <small class="text-muted d-block mb-1">Smart Followers</small>
                ${smartFollowerItems.join('')}
            </div>
        `);
    }
    
    // 3. YAPS (그룹)
    if (data.yaps) {
        let yapsItems = [];
        if (data.yaps.yaps_all !== null && data.yaps.yaps_all !== undefined) {
            yapsItems.push(`<div class="stat-item"><span class="stat-label">ALL</span><strong>${Math.round(data.yaps.yaps_all).toLocaleString()}</strong></div>`);
        }
        if (data.yaps.yaps_l30d !== null && data.yaps.yaps_l30d !== undefined && data.yaps.yaps_l30d > 0) {
            yapsItems.push(`<div class="stat-item"><span class="stat-label">30D</span><strong>${Math.round(data.yaps.yaps_l30d).toLocaleString()}</strong></div>`);
        }
        if (data.yaps.yaps_l7d !== null && data.yaps.yaps_l7d !== undefined && data.yaps.yaps_l7d > 0) {
            yapsItems.push(`<div class="stat-item"><span class="stat-label">7D</span><strong>${Math.round(data.yaps.yaps_l7d).toLocaleString()}</strong></div>`);
        }
        if (data.yaps.yaps_l24h !== null && data.yaps.yaps_l24h !== undefined && data.yaps.yaps_l24h > 0) {
            yapsItems.push(`<div class="stat-item"><span class="stat-label">24H</span><strong>${Math.round(data.yaps.yaps_l24h).toLocaleString()}</strong></div>`);
        }
        
        if (yapsItems.length > 0) {
            stats.push(`
                <div class="stat-group">
                    <small class="text-muted d-block mb-1">YAPS</small>
                    ${yapsItems.join('')}
                </div>
            `);
        }
    }
    
    // 4. Wallchain (X Score)
    if (user.wal_score) {
        stats.push(`
            <div class="stat-group">
                <small class="text-muted d-block mb-1">Wallchain</small>
                <div class="stat-item"><span class="stat-label">X SCORE</span><strong>${user.wal_score.toLocaleString()}</strong></div>
            </div>
        `);
    }
    
    // 5. Leaderboard 개수 (그룹)
    const kaitoCount = data.kaito_projects ? Object.keys(data.kaito_projects).length : 0;
    const cookieCount = data.cookie_projects ? Object.keys(data.cookie_projects).length : 0;
    const wallchainCount = data.wallchain_projects ? Object.keys(data.wallchain_projects).length : 0;
    
    if (kaitoCount > 0 || cookieCount > 0 || wallchainCount > 0) {
        let leaderboardItems = [];
        if (kaitoCount > 0) {
            leaderboardItems.push(`<div class="stat-item"><span class="stat-label"><img src="/static/kaito.png" alt="Kaito" style="width: 16px; height: 16px; margin-right: 4px;">Kaito LB</span><strong>${kaitoCount}</strong></div>`);
        }
        if (cookieCount > 0) {
            leaderboardItems.push(`<div class="stat-item"><span class="stat-label">🍪 Cookie LB</span><strong>${cookieCount}</strong></div>`);
        }
        if (wallchainCount > 0) {
            leaderboardItems.push(`<div class="stat-item"><span class="stat-label">🦆 Wallchain LB</span><strong>${wallchainCount}</strong></div>`);
        }
        
        stats.push(`
            <div class="stat-group">
                <small class="text-muted d-block mb-1">Leaderboards</small>
                ${leaderboardItems.join('')}
            </div>
        `);
    }
    
    // Kaito 이미지 ID 감지 (숫자만 있는 경우)
    let imageUrl = user.imageUrl;
    if (imageUrl && /^\d+$/.test(imageUrl)) {
        // 숫자만 있으면 Kaito 이미지 ID로 간주하고 서버 프록시 사용
        imageUrl = `/kaito-img/${imageUrl}`;
    }
    
    let html = `
        <div class="card shadow-sm mb-4">
            <div class="card-body">
                <div class="row align-items-center">
                    ${imageUrl ? `
                        <div class="col-auto">
                            <img src="${imageUrl}" alt="${user.displayName}" 
                                 style="width: 80px; height: 80px; border-radius: 50%;" onerror="this.style.display='none'">
                        </div>
                    ` : ''}
                    <div class="col">
                        <h3 class="mb-1">
                            <a href="https://x.com/${user.infoName}" target="_blank" class="text-decoration-none">
                                ${user.displayName || user.infoName}
                            </a>
                        </h3>
                        <p class="text-muted mb-2">
                            <a href="https://x.com/${user.infoName}" target="_blank" class="text-decoration-none">
                              @${user.infoName}
                            </a>
                        </p>
                        <div class="d-flex gap-4 flex-wrap">
                            ${stats.join('')}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    // 모든 프로젝트를 하나의 그리드에 표시
    html += `<div class="projects-grid">`;
    
    // Kaito 프로젝트 (최우선)
    if (data.kaito_projects && Object.keys(data.kaito_projects).length > 0) {
        Object.keys(data.kaito_projects).sort().forEach(projectName => {
            const rankings = sortTimeframes(data.kaito_projects[projectName]);
            const projectShortName = projectName.replace('kaito-', '');
            const displayName = projectShortName.toUpperCase();
            
            // 순위가 없으면 카드를 표시하지 않음
            if (rankings.length === 0) {
                return;
            }
            
            html += `<div class="card project-card kaito-card">
                <div class="card-body">
                    <span class="project-type-icon kaito"><img src="/static/kaito.png" alt="Kaito" style="width: 70%; height: 70%; object-fit: contain;"></span>
                    <a href="/kaito/${projectShortName}/user/${user.infoName}" class="user-detail-link" title="유저 상세 분석">🔍</a>
                    <h5 class="card-title"><span class="flag-emoji">🌐</span><span>${displayName}</span></h5>
                    <div class="timeframe-container">`;
            
            rankings.forEach(r => {
                const displayTimeframe = r.timeframe;
                
                html += `<span class="timeframe-badge">
                    <span class="timeframe-label">${displayTimeframe}</span>
                    <span class="rank-info">#${r.msRank}</span>
                    <span class="percent-info">${r.ms ? `${r.ms.toFixed(3)}%` : ''}</span>
                </span>`;
            });
            
            html += `</div></div></div>`;
        });
    }
    
    // Cookie 프로젝트
    if (Object.keys(data.cookie_projects).length > 0) {
        Object.keys(data.cookie_projects).sort().forEach(projectName => {
            const rankings = sortTimeframes(data.cookie_projects[projectName]);
            
            // 마쉐 랭킹 (ms > 0인 경우만)
            const msRankings = rankings.filter(r => r.msRank && r.ms > 0);
            
            // c마쉐 랭킹 (cms > 0인 경우만)
            const cmsRankings = rankings.filter(r => r.cmsRank && r.cms > 0);
            
            // 마쉐 카드
            if (msRankings.length > 0) {
                const displayName = formatCookieProjectName(projectName, '(MS)');
                html += `<div class="card project-card">
                    <div class="card-body">
                        <span class="project-type-icon">🍪</span>
                        <a href="/cookie/${projectName}/user/${user.infoName}?metric=snapsPercent" class="user-detail-link" title="유저 상세 분석">🔍</a>
                        <h5 class="card-title">${displayName}</h5>
                        <div class="timeframe-container">`;
                
                msRankings.forEach(r => {
                    html += `<span class="timeframe-badge">
                        <span class="timeframe-label">${r.timeframe}</span>
                        <span class="rank-info">#${r.msRank}</span>
                        <span class="percent-info">${r.ms ? `${r.ms.toFixed(3)}%` : ''}</span>
                    </span>`;
                });
                
                html += `</div></div></div>`;
            }
            
            // c마쉐 카드 (별도 카드)
            if (cmsRankings.length > 0) {
                const displayName = formatCookieProjectName(projectName, '(cMS)');
                html += `<div class="card project-card">
                    <div class="card-body">
                        <span class="project-type-icon">🍪</span>
                        <a href="/cookie/${projectName}/user/${user.infoName}?metric=cSnapsPercent" class="user-detail-link" title="유저 상세 분석">🔍</a>
                        <h5 class="card-title">${displayName}</h5>
                        <div class="timeframe-container">`;
                
                cmsRankings.forEach(r => {
                    html += `<span class="timeframe-badge">
                        <span class="timeframe-label">${r.timeframe}</span>
                        <span class="rank-info">#${r.cmsRank}</span>
                        <span class="percent-info">${r.cms ? `${r.cms.toFixed(3)}%` : ''}</span>
                    </span>`;
                });
                
                html += `</div></div></div>`;
            }
        });
    }
    
    // Wallchain 프로젝트
    if (Object.keys(data.wallchain_projects).length > 0) {
        Object.keys(data.wallchain_projects).sort().forEach(projectName => {
            const rankings = sortTimeframes(data.wallchain_projects[projectName]);
            const projectShortName = projectName.replace('wallchain-', '');
            const displayName = projectShortName.toUpperCase();
            
            // 순위가 없으면 카드를 표시하지 않음
            if (rankings.length === 0) {
                return;
            }
            
            html += `<div class="card project-card wallchain-card">
                <div class="card-body">
                    <span class="project-type-icon wallchain">🦆</span>
                    <a href="/wallchain/${projectShortName}/user/${user.infoName}" class="user-detail-link" title="유저 상세 분석">🔍</a>
                    <h5 class="card-title"><span class="flag-emoji">🌐</span><span>${displayName}</span></h5>
                    <div class="timeframe-container">`;
            
            rankings.forEach(r => {
                const changeIcon = r.positionChange > 0 ? '↑' : r.positionChange < 0 ? '↓' : '';
                const changeColor = r.positionChange > 0 ? 'success' : r.positionChange < 0 ? 'danger' : 'secondary';
                const displayTimeframe = r.timeframe.replace('epoch-2', 'epoch2').replace('epoch_2', 'epoch2').replace('epoch-1', 'epoch1');
                
                let changeDisplay = '';
                if (r.positionChange === 'new')
                    changeDisplay = `<span class="position-change text-success">NEW</span>`;
                else if (r.positionChange !== null) {
                    if (r.positionChange === 0) {
                        changeDisplay = `<span class="position-change text-${changeColor}">(0)</span>`;
                    } else {
                        changeDisplay = `<span class="position-change text-${changeColor}">(${changeIcon}${Math.abs(r.positionChange)})</span>`;
                    }
                }

                
                html += `<span class="timeframe-badge">
                    <span class="timeframe-label">${displayTimeframe}</span>
                    <span class="rank-info">#${r.msRank} ${changeDisplay}</span>
                    <span class="percent-info">${r.ms ? `${r.ms.toFixed(3)}%` : ''}</span>
                </span>`;
            });
            
            html += `</div></div></div>`;
        });
    }
    
    // 그리드 닫기
    html += `</div>`;
    
    if (Object.keys(data.cookie_projects).length === 0 && Object.keys(data.wallchain_projects).length === 0 && (!data.kaito_projects || Object.keys(data.kaito_projects).length === 0)) {
        html += `<div class="alert alert-info">이 사용자는 어떤 프로젝트에도 없습니다.</div>`;
    }
    
    searchResults.innerHTML = html;
    twemoji.parse(document.body);
}

// 페이지 로드 시 URL에서 username 확인하고 자동 검색
window.addEventListener('DOMContentLoaded', function() {
    const urlParams = new URLSearchParams(window.location.search);
    const username = urlParams.get('username');
    if (username) {
        searchInput.value = username;
        loadUserData(username);
    }
});

// 브라우저 뒤로가기/앞으로가기 처리
window.addEventListener('popstate', function(event) {
    const urlParams = new URLSearchParams(window.location.search);
    const username = urlParams.get('username');
    
    if (username) {
        searchInput.value = username;
        loadUserData(username);
    } else {
        // username이 없으면 검색 결과 초기화
        searchResults.innerHTML = '';
        searchInput.value = '';
        lastSearchedUsername = '';
    }
});
