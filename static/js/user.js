// user.html 전용 JavaScript

// 사용자 검색 기능
$(document).ready(function() {
    const allUsers = window.allUsersData || [];
    const searchInput = document.getElementById('userSearchInput');
    const searchResults = document.getElementById('searchResults');
    
    if (searchInput && searchResults) {
        searchInput.addEventListener('input', function() {
            const searchVal = this.value.toLowerCase();
            
            if (searchVal.length < 2) {
                searchResults.style.display = 'none';
                return;
            }
            
            const filteredUsers = allUsers.filter(user => {
                return user.displayName.toLowerCase().includes(searchVal) || 
                       user.username.toLowerCase().includes(searchVal);
            }).slice(0, 10);
            
            if (filteredUsers.length > 0) {
                searchResults.innerHTML = '';
                filteredUsers.forEach(user => {
                    const item = document.createElement('div');
                    item.className = 'search-result-item';
                    item.innerHTML = `<strong>${user.displayName}</strong> <span class="text-muted">@${user.username}</span>`;
                    item.addEventListener('click', function() {
                        const currentMetric = document.getElementById('metric') ? document.getElementById('metric').value : 'snapsPercent';
                        const currentTimeframe = window.currentTimeframe || 'TOTAL';
                        window.location.href = `/${window.currentProject}/user/${user.username}?timeframe=${currentTimeframe}&metric=${currentMetric}`;
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
    }
});
