// user.html 전용 JavaScript

// 사용자 검색 기능
$(document).ready(function() {
    const allUsers = window.allUsersData || [];
    const searchInput = document.getElementById('userSearchInput');
    const searchResults = document.getElementById('searchResults');
    const isWallchain = window.isWallchain || false;
    
    console.log('user.js loaded');
    console.log('allUsers:', allUsers);
    console.log('allUsers length:', allUsers.length);
    console.log('searchInput:', searchInput);
    console.log('searchResults:', searchResults);
    console.log('isWallchain:', isWallchain);
    
    if (searchInput && searchResults) {
        console.log('Search elements found, adding event listener');
        
        searchInput.addEventListener('input', function() {
            const searchVal = this.value.toLowerCase();
            console.log('Input event, searchVal:', searchVal);
            
            if (searchVal.length < 2) {
                searchResults.style.display = 'none';
                return;
            }
            
            const filteredUsers = allUsers.filter(user => {
                const displayName = user.displayName || user.name || user.username;
                const matches = displayName.toLowerCase().includes(searchVal) || 
                       user.username.toLowerCase().includes(searchVal);
                return matches;
            }).slice(0, 10);
            
            console.log('filteredUsers:', filteredUsers);
            
            if (filteredUsers.length > 0) {
                searchResults.innerHTML = '';
                filteredUsers.forEach(user => {
                    const item = document.createElement('div');
                    item.className = 'search-result-item';
                    const displayName = user.displayName || user.name || user.username;
                    item.innerHTML = `<strong>${displayName}</strong> <span class="text-muted">@${user.username}</span>`;
                    item.addEventListener('click', function() {
                        const currentMetric = document.getElementById('metric') ? document.getElementById('metric').value : 'snapsPercent';
                        const currentTimeframe = window.currentTimeframe || 'TOTAL';
                        
                        // wallchain 여부에 따라 URL 경로 다르게 구성
                        if (isWallchain) {
                            window.location.href = `/wallchain/${window.currentProject}/user/${user.username}?timeframe=${currentTimeframe}`;
                        } else {
                            window.location.href = `/${window.currentProject}/user/${user.username}?timeframe=${currentTimeframe}&metric=${currentMetric}`;
                        }
                    });
                    searchResults.appendChild(item);
                });
                searchResults.style.display = 'block';
            } else {
                searchResults.style.display = 'none';
            }
        });
        
        document.addEventListener('click', function(e) {
            if (e.target !== searchInput && e.target.parentNode !== searchResults) {
                searchResults.style.display = 'none';
            }
        });
        
        searchInput.addEventListener('focus', function() {
            if (this.value.length >= 2) {
                searchResults.style.display = 'block';
            }
        });
    } else {
        console.error('Search elements not found!');
    }
});

