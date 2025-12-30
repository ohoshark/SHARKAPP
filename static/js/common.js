// 지갑 주소 복사 함수
function copyWalletAddress() {
    const addr = "0xC8e695B30EF512AE634007efa89A75E1421e3055";
    
    navigator.clipboard.writeText(addr).then(() => {
        const toastElement = document.getElementById('copyToast');
        const toast = new bootstrap.Toast(toastElement, {
            autohide: true,
            delay: 1000
        });
        toast.show();
    }).catch(err => {
        console.error('복사 실패:', err);
    });
}

// Twemoji 초기화
$(document).ready(function() {
    twemoji.parse(document.body);
});

// 차트 로딩 오버레이 숨기기
window.addEventListener('load', function() {
    const chartLoadingOverlay = document.getElementById('chart-loading-overlay');
    if (chartLoadingOverlay) {
        chartLoadingOverlay.style.display = 'none';
    }
});

// 다크모드 토글 기능
(function() {
    // 테마 아이콘 업데이트
    function updateThemeIcon() {
        const themeIcon = document.getElementById('themeIcon');
        if (themeIcon) {
            if (document.documentElement.classList.contains('dark-mode')) {
                themeIcon.className = 'fas fa-sun';
            } else {
                themeIcon.className = 'fas fa-moon';
            }
        }
    }
    
    // 페이지 로드 시 아이콘 업데이트 및 이벤트 등록
    document.addEventListener('DOMContentLoaded', function() {
        updateThemeIcon();
        
        // 테마 토글 버튼 이벤트
        const themeToggle = document.getElementById('themeToggle');
        if (themeToggle) {
            themeToggle.addEventListener('click', function() {
                document.documentElement.classList.toggle('dark-mode');
                
                // 테마 저장
                const newTheme = document.documentElement.classList.contains('dark-mode') ? 'dark' : 'light';
                localStorage.setItem('theme', newTheme);
                
                // 아이콘 업데이트
                updateThemeIcon();
            });
        }
        
        // noUiSlider 핸들 위치에 따른 강조 효과
        const sliders = document.querySelectorAll('.noUi-target');
        sliders.forEach(slider => {
            if (slider.noUiSlider) {
                slider.noUiSlider.on('update', function(values, handle) {
                    const handles = slider.querySelectorAll('.noUi-handle');
                    handles.forEach((handleEl, index) => {
                        const value = parseFloat(values[index]);
                        const min = slider.noUiSlider.options.range.min;
                        const max = slider.noUiSlider.options.range.max;
                        
                        // 최소값 또는 최대값에 있을 때 active 클래스 추가
                        if (value === min || value === max) {
                            handleEl.classList.add('active');
                        } else {
                            handleEl.classList.remove('active');
                        }
                    });
                });
            }
        });
    });
})();
