    // 사용자 검색 기능
    document.getElementById('userSearch').addEventListener('input', function() {
        const searchVal = this.value.toLowerCase();
        const userItems = document.getElementsByClassName('user-item');
        
        for (let i = 0; i < userItems.length; i++) {
            const userText = userItems[i].textContent.toLowerCase();
            if (userText.indexOf(searchVal) > -1) {
                userItems[i].style.display = "";
            } else {
                userItems[i].style.display = "none";
            }
        }
    });
