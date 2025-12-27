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
