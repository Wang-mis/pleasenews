from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter


if __name__ == '__main__':
    url = "https://www.nytimes.com/2024/09/18/world/asia/taiwan-pagers-lebanon.html"
    
    PROXIES = {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    }

    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7,en-GB;q=0.6,en-US;q=0.5',
        'Cache-Control': 'max-age=0',
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
        'Cookie': r'nyt-a=yslptueUyh390lUuMUB1CE; _cb=BkXLeVB27HwDlDI6c; _gcl_au=1.1.1788262337.1728382037; nyt-gdpr=0; nyt-counter=1; nyt-tos-viewed=true; purr-pref-agent=<G_<C_<T0<Tp1_<Tp2_<Tp3_<Tp4_<Tp7_<a12; nyt-purr=cfhhcfhhhckfhcfhhgah2; g_state={"i_p":1730095522390,"i_l":1}; nyt-us=0; nyt-geo=HK; _ga=GA1.1.966910138.1730088415; checkout_entry_point=direct; ath_content_edition=uk; ath_anonymous_user_id=17300884149345618251; iter_id=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhaWQiOiI2NzA1MDQ1NTQxMGY1MTA4YmM3OTZhNTYiLCJhaWRfZXh0Ijp0cnVlLCJjb21wYW55X2lkIjoiNWMwOThiM2QxNjU0YzEwMDAxMmM2OGY5IiwiaWF0IjoxNzMwMDg4NjIxfQ.r0T5DgIYPdMRhpECkxKGiCw-OcKZH7KlL2R--CEaFow; _ga_EKN0VKJGMQ=GS1.1.1730088414.1.0.1730088926.60.0.0; _v__chartbeat3=Br6dRDCSChD0Uc0kx; purr-cache=<G_<C_<T0<Tp1_<Tp2_<Tp3_<Tp4_<Tp7_<a0_<K0<S0<r<ur; NYT-MPS=0000000cc73d0055888cea3b169f54474f1553c42e627584a2146320d12c00141bd52940d349a3ce212756f08a422c2b5d0943a40f323c7321a7b54b412b8f; nyt-auth-method=username; RT="z=1&dm=nytimes.com&si=0b558ac3-cdc8-4efa-b8b9-f5c481535dfc&ss=m2shy3hu&sl=2&tt=26z&bcn=%2F%2F684d0d47.akstat.io%2F&nu=169tn99b1&cl=nb3g&ul=o0ca&hd=o0m7"; _fbp=fb.1.1730089495109.401457472732908316; _scid=QnjD7HmtfCr3-qBZqlnCiSqNj2ZXOWoR; _sctr=1%7C1730044800000; regi_cookie=; _scid_r=XvjD7HmtfCr3-qBZqlnCiSqNj2ZXOWoRZUHfyw; _rdt_uuid=1730089495175.a402bd5b-c34b-4928-8c97-69a444ac4748; _ScCbts=%5B%5D; nyt-b3-traceid=f9bfe13194d74360803995e2ca305dbc; nyt-jkidd=uid=263822989&lastRequest=1730090108623&activeDays=%5B0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C1%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C1%5D&adv=2&a7dv=1&a14dv=1&a21dv=2&lastKnownType=sub&newsStartDate=1730075363&entitlements=AAA+ATH+AUD+CKG+MM+MOW+MSD+MTD+WC+XWD; _chartbeat2=.1728382036627.1730090108595.0000000000000001.C8A38XCGVETwD7__ySBUltmOCI-A8A.1; _cb_svref=external; datadome=jjv8EeLoN3EfaLaqxUSJcQlWn_FRR7O~TiX7fEWTm7IxcvLxFq5yd5ukmsi6Gn1fM~vzqjKVS_uR22jzKitjRI7OXDpIS5sab1nqyaEzqhBTf1D6Gn~sfbWSp9nI3IFt; NYT-S=0^CBkSMQiUpPy4BhCNqvy4BhoSMS2PQdKz962L8eP-6N_f8GTLII295n0qAh53OJSk_LgGQgAaQLxaZMG-BqXbxqek9LEH0C73JexF8sq-EaMMRu5jm4eWYlfVQCdW2Sya2OyeZ3tVkEGW3XjYrbmmucEVLNlfUg4=; SIDNY=CBkSMQiUpPy4BhCNqvy4BhoSMS2PQdKz962L8eP-6N_f8GTLII295n0qAh53OJSk_LgGQgAaQLxaZMG-BqXbxqek9LEH0C73JexF8sq-EaMMRu5jm4eWYlfVQCdW2Sya2OyeZ3tVkEGW3XjYrbmmucEVLNlfUg4=; _dd_s=rum=0&expire=1730091070791&logs=1&id=0ec2553c-f9b9-47dd-b43d-07fed0e6c979&created=1730088414846',
    }

    sess = requests.Session()
    sess.mount('http://', HTTPAdapter(max_retries=10))
    sess.mount('https://', HTTPAdapter(max_retries=10))
    sess.keep_alive = False
    requests.packages.urllib3.disable_warnings()
    req = requests.get(url, proxies=PROXIES, headers=HEADERS, timeout=10, verify=False)
    req.close()

    content = req.text
    soup = BeautifulSoup(content, 'lxml')
    print(soup)
    # print(soup.select("h1.entry-title"))

