#echo "Chaning templates..."
#for resource in sso73-image-stream.json \
#  sso73-https.json \
#  sso73-mysql.json \
#  sso73-mysql-persistent.json \
#  sso73-postgresql.json \
#  sso73-postgresql-persistent.json \
#  sso73-x509-https.json \
#  sso73-x509-mysql-persistent.json \
#  sso73-x509-postgresql-persistent.json
#do
#  oc replace -n openshift --force -f \
#  https://raw.githubusercontent.com/jboss-container-images/redhat-sso-7-openshift-image/sso73-dev/templates/${resource}
#done
#
#echo "Changing to openshift namespace"
#oc project openshift
#
#IMAGE=registry-proxy.engineering.redhat.com/rh-osbs/redhat-sso-7-sso73-openshift:1.0-21
#oc import-image redhat-sso73-openshift:1.0 --confirm --insecure --from=$IMAGE
#oc tag --insecure $IMAGE redhat-sso73-openshift:1.0
#
#oc project oauth2-cluster-test

echo "Deploying template..."
oc new-app --template=sso73-x509-https

# sed -n "s@.*RH-SSO Administrator Username=\(.*\) #@\1@p"
# sed -n "s@.*RH-SSO Administrator Password=\(.*\) # generated@\1@p"
