package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"runtime"
	"testing"
	"time"

	"gatewright/internal/tlsutil"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, err := tlsutil.GenerateSelfSignedCert(dir, []string{"gateway.test", "10.1.2.3"})
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert: %v", err)
	}
	if certPath == "" || keyPath == "" {
		t.Fatal("empty output paths")
	}

	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("read cert: %v", err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}

	// Permissions must be 0600. On Windows the POSIX mode bits are a thin
	// ACL projection (os.Chmod can only toggle the read-only bit), so the
	// exact-mode assertion only holds on Unix.
	for _, p := range []string{certPath, keyPath} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if runtime.GOOS != "windows" {
			if perm := fi.Mode().Perm(); perm != 0o600 {
				t.Errorf("%s permissions = %v, want 0600", p, perm)
			}
		}
	}

	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("cert.pem: no CERTIFICATE PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}

	if cert.PublicKeyAlgorithm != x509.ECDSA {
		t.Errorf("public key algorithm = %v, want ECDSA", cert.PublicKeyAlgorithm)
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("public key = %T, want *ecdsa.PublicKey", cert.PublicKey)
	}
	if pub.Curve != elliptic.P256() {
		t.Errorf("curve = %v, want P-256", pub.Curve)
	}

	validity := cert.NotAfter.Sub(cert.NotBefore)
	if validity < 6*24*time.Hour || validity > 8*24*time.Hour {
		t.Errorf("validity = %s, want ~7 days", validity)
	}
	if time.Until(cert.NotAfter) > 7*24*time.Hour+time.Hour {
		t.Errorf("NotAfter too far out: %s", cert.NotAfter)
	}

	wantDNS := map[string]bool{"gateway.test": true, "localhost": true}
	gotDNS := map[string]bool{}
	for _, d := range cert.DNSNames {
		gotDNS[d] = true
	}
	for d := range wantDNS {
		if !gotDNS[d] {
			t.Errorf("missing DNS SAN %q (got %v)", d, cert.DNSNames)
		}
	}
	wantIP := map[string]bool{"127.0.0.1": true, "10.1.2.3": true}
	gotIP := map[string]bool{}
	for _, ip := range cert.IPAddresses {
		gotIP[ip.String()] = true
	}
	for ip := range wantIP {
		if !gotIP[ip] {
			t.Errorf("missing IP SAN %q (got %v)", ip, cert.IPAddresses)
		}
	}

	if !cert.IsCA || !cert.BasicConstraintsValid {
		t.Error("certificate must be its own CA with BasicConstraintsValid")
	}
	if !time.Now().Add(-time.Minute).After(cert.NotBefore.Add(-time.Minute)) {
		// NotBefore backdated; just ensure it is in the past so it works now.
		t.Errorf("NotBefore %s is not in the past", cert.NotBefore)
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil || keyBlock.Type != "EC PRIVATE KEY" {
		t.Fatalf("key.pem: no EC PRIVATE KEY PEM block")
	}
	if _, err := x509.ParseECPrivateKey(keyBlock.Bytes); err != nil {
		t.Errorf("parse private key: %v", err)
	}

	// The pair must load as a usable server certificate.
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		t.Errorf("X509KeyPair: %v", err)
	}
}

func TestGenerateSelfSignedCertDeduplicatesSANs(t *testing.T) {
	dir := t.TempDir()
	_, _, err := tlsutil.GenerateSelfSignedCert(dir, []string{"localhost", "localhost", "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	// A second generation overwrites the files without error.
	certPath, _, err := tlsutil.GenerateSelfSignedCert(dir, []string{"again.test"})
	if err != nil {
		t.Fatalf("regenerate: %v", err)
	}
	data, err := os.ReadFile(certPath)
	if err != nil || len(data) == 0 {
		t.Fatalf("regenerated cert unreadable: %v", err)
	}
}
