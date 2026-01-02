let searchTimeout;
let selectedIndex = -1; // 키보드로 선택된 항목 인덱스
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
                
                autocompleteDropdown.innerHTML = data.map(user => `
                    <div class="autocomplete-item" data-username="${user.infoName}">
                        ${user.imageUrl ? `<img src="${user.imageUrl}" alt="${user.displayName || user.infoName}" onerror="this.style.display='none'">` : ''}
                        <div>
                            <strong>${user.displayName || user.infoName}</strong>
                            <div class="text-muted">@${user.infoName}</div>
                        </div>
                    </div>
                `).join('');
                
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

// 외부 클릭 시 드롭다운 닫기
document.addEventListener('click', function(e) {
    if (!searchInput.contains(e.target) && !autocompleteDropdown.contains(e.target)) {
        autocompleteDropdown.style.display = 'none';
        selectedIndex = -1;
    }
});

// 검색 버튼 클릭
searchButton.addEventListener('click', function() {
    let username = searchInput.value.trim();
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
    
    // URL에 username 추가
    const url = new URL(window.location);
    url.searchParams.set('username', username);
    window.history.pushState({}, '', url);
    
    fetch(`/api/user-data/${encodeURIComponent(username)}`)
        .then(res => {
            if (!res.ok) {
                throw new Error('Network response was not ok');
            }
            return res.json();
        })
        .then(data => {
            if (!data || data.error) {
                searchResults.innerHTML = `
                    <div class="alert alert-warning text-center">
                        <i class="fas fa-exclamation-triangle"></i> 사용자의 정보가 존재하지 않습니다
                    </div>
                `;
                return;
            }
            
            renderUserData(data);
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
    let html = `
        <div class="card shadow-sm mb-4">
            <div class="card-body">
                <div class="row align-items-center">
                    ${user.imageUrl ? `
                        <div class="col-auto">
                            <img src="${user.imageUrl}" alt="${user.displayName}" 
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
                            ${user.follower ? `<div><small class="text-muted d-block">Followers</small><strong>${user.follower.toLocaleString()}</strong></div>` : ''}
                            ${user.kaito_smart_follower ? `<div><small class="text-muted d-block">🤖 Smart Followers</small><strong>${user.kaito_smart_follower.toLocaleString()}</strong></div>` : ''}
                            ${user.cookie_smart_follower ? `<div><small class="text-muted d-block">🍪 Smart Followers</small><strong>${user.cookie_smart_follower.toLocaleString()}</strong></div>` : ''}
                            ${user.wal_score ? `<div><small class="text-muted d-block">🦆 X SCORE</small><strong>${user.wal_score.toLocaleString()}</strong></div>` : ''}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    // 모든 프로젝트를 하나의 그리드에 표시
    html += `<div class="projects-grid">`;
    
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
                const displayTimeframe = r.timeframe.replace('epoch-2', 'epoch2').replace('epoch_2', 'epoch2');
                
                let changeDisplay = '';
                if (r.positionChange !== null) {
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
    
    // Kaito 프로젝트
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
                    <span class="project-type-icon kaito">🤖</span>
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
