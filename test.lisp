;;;;; -*- mode: common-lisp;   common-lisp-style: modern;    coding: utf-8; -*-
;;;;;
;;;;;
;;;;; redis mediator test suite
;;;;;
;;;;; Author:      Dan Lentz, Lentz Intergalactic Softworks, Wed Jul 27 08:12:05 2011
;;;;; Maintainer:  <danlentz@gmail.com>
;;;;;

(in-package :red)
(in-readtable :rx)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TEST SUITE
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(unless (find-test 'gs.ebu.resource) 
  (def suite  (gs.ebu.resource       :in root-suite        :documentation "")))

(def suite* (gs.ebu.resource.redis :in gs.ebu.resource   :documentation "")) 



(def test test/context-key/format/0 ()
  (is (string-equal "context:x" (print-context-key "x"))))



(def test test/uuid-print-urn/0 ()
  (is (string-equal "urn:uuid:DED2652F-4BDA-471C-8E15-F94DFAA990AD"
        (uuid-print-urn (uuid:make-uuid-from-string "DED2652F-4BDA-471C-8E15-F94DFAA990AD"))))
  (is (string-equal "urn:uuid:00000000-0000-0000-0000-000000000000"
        (uuid-print-urn (uuid:make-null-uuid)))))



(def test test/urn-string-p/0 ()
  (is (urn-string-p (uuid-print-urn (uuid:make-v4-uuid))))
  (is (not (urn-string-p "http://example.com/")))
  (is (urn-string-p (uuid-print-urn (uuid:make-null-uuid))))
  (is (not (urn-string-p "banana cream pie"))))

(def test test/uuid-parse-urn/0 ()
  (is
    (string-equal
      (uuid:print-bytes nil (uuid-parse-urn "urn:uuid:DED2652F-4BDA-471C-8E15-F94DFAA990AD"))
      (uuid:print-bytes nil (uuid:make-uuid-from-string "DED2652F-4BDA-471C-8E15-F94DFAA990AD")))))

(def test test/uuid-parse-urn/1 ()
  (dotimes (i 10)
    (let* ((uuid (uuid:make-v4-uuid))
            (uuid-bytes (uuid:print-bytes nil uuid)))
      (is (string-equal
            uuid-bytes
            (uuid:print-bytes nil (uuid-parse-urn (uuid-print-urn uuid))))))))
    
(def test test/uuid-parse-urn/2 ()
  (signals error (uuid-parse-urn "key-lime pie"))
  (signals error (uuid-parse-urn "urn:uuid:DED2652F4BDA471C8E15F94DFAA990AD"))
  (signals error (uuid-parse-urn "urn:sha1:00000000-0000-0000-0000-000000000000"))
  (signals error (uuid-parse-urn "urn:uuid:00000000-0000-0000-0000-00000000000")))



(def test test/context-id/0 ()
  "test context id generation for native (model) uri/uuid objects"
  (is (string-equal "00000000000000000000000000000000"
        (context-id (uuid:make-null-uuid))))
  (is (string-equal "C316495DFDD45DAD9B1DB005F540542D"
        (context-id (puri:uri "http://example.com/")))))

(def test test/context-id/1 ()
  "test context id generation for serialized (repository) uri/uuid strings"
  (is (string-equal "00000000000000000000000000000000"
        (context-id (uuid-print-urn (uuid:make-null-uuid)))))
  (is (string-equal "C316495DFDD45DAD9B1DB005F540542D"
        (context-id "http://example.com/"))))

(def test test/context-id/2 ()
  "test context id equivalence of uri object and uri string representations"
  (dolist (uri (list
                 "http://www.google.com/"
                 "http://ibm.com/"
                 "http://github.com/danlentz/"
                 "http://www.w3.org/ns/earl#"
                 "http://purl.org/dc/elements/1.1/"
                 "http://ebu.gs/resource/user#"))
    (is (string-equal
          (context-id (puri:uri uri))
          (context-id uri)))))

(def test test/context-id/3 ()
  "test context id equivalence of uuid object and urn string representations"
  (dotimes (i 10)
    (let ((uuid (uuid:make-v4-uuid)))
      (is (string-equal
            (context-id uuid)
            (context-id (uuid-print-urn uuid)))))))


(def test test/red-db-find-context/0 ()
  (is (not (red-db-find-context (red-db) "blueberry-pie")))
  (is (not (red-db-find-context (red-db) (puri:uri "http://blueberry-pie.net/"))))
  (is (not (red-db-find-context (red-db) uuid:+namespace-x500+)))
  (is (not (red-db-find-context (red-db) (uuid-print-urn uuid:+namespace-x500+)))))


(def test test/red-db-*-context/uri-object/0 ()
  (flet ((every-string-equal (list1 list2)
           (is (eql (length list1) (length list2)))
           (loop
             :for string1 :in list1
             :for string2 :in list2
             :do (is (string-equal string1 string2)))))
    (every-string-equal
      (multiple-value-list
        (red-db-add-context (red-db) (puri:uri "http://example.com/")))
      (list "http://example.com/" "context:C316495DFDD45DAD9B1DB005F540542D"))
    (every-string-equal
      (multiple-value-list
        (red-db-find-context (red-db) (puri:uri "http://example.com/")))
      (list "http://example.com/" "context:C316495DFDD45DAD9B1DB005F540542D"))
    (every-string-equal
      (multiple-value-list 
        (red-db-delete-context (red-db) (puri:uri "http://example.com/")))
      (list "http://example.com/" "context:C316495DFDD45DAD9B1DB005F540542D"))
    (is (not (red-db-find-context (red-db) (puri:uri "http://example.com/"))))
    (is (not (red-db-delete-context (red-db) (puri:uri "http://example.com/"))))))

(def test test/red-db-*-context/uri-string/0 ()
  (flet ((every-string-equal (list1 list2)
           (is (eql (length list1) (length list2)))
           (loop
             :for string1 :in list1
             :for string2 :in list2
             :do (is (string-equal string1 string2)))))
    (every-string-equal
      (multiple-value-list
        (red-db-add-context (red-db) "http://example.com/"))
      (list "http://example.com/" "context:C316495DFDD45DAD9B1DB005F540542D"))
    (every-string-equal
      (multiple-value-list
        (red-db-find-context (red-db) "http://example.com/"))
      (list "http://example.com/" "context:C316495DFDD45DAD9B1DB005F540542D"))
    (every-string-equal
      (multiple-value-list 
        (red-db-delete-context (red-db) "http://example.com/"))
      (list "http://example.com/" "context:C316495DFDD45DAD9B1DB005F540542D"))
    (is (not (red-db-find-context (red-db)  "http://example.com/")))
    (is (not (red-db-delete-context (red-db) "http://example.com/")))))

(def test test/red-db-*-context/uuid-object/0 ()
  (flet ((every-string-equal (list1 list2)
           (is (eql (length list1) (length list2)))
           (loop
             :for string1 :in list1
             :for string2 :in list2
             :do (is (string-equal string1 string2)))))
    (every-string-equal
      (multiple-value-list
        (red-db-add-context (red-db) uuid:+namespace-dns+))
      (list "urn:uuid:6BA7B810-9DAD-11D1-80B4-00C04FD430C8"
        "context:6BA7B8109DAD11D180B400C04FD430C8"))
    (every-string-equal
      (multiple-value-list
        (red-db-find-context (red-db) uuid:+namespace-dns+))
      (list "urn:uuid:6BA7B810-9DAD-11D1-80B4-00C04FD430C8"
        "context:6BA7B8109DAD11D180B400C04FD430C8"))
    (every-string-equal
      (multiple-value-list
        (red-db-delete-context (red-db) uuid:+namespace-dns+))
      (list "urn:uuid:6BA7B810-9DAD-11D1-80B4-00C04FD430C8"
        "context:6BA7B8109DAD11D180B400C04FD430C8"))
    (is (not (red-db-find-context (red-db) uuid:+namespace-dns+)))
    (is (not (red-db-delete-context (red-db) uuid:+namespace-dns+)))))

(def test test/red-db-*-context/urn-string/0 ()
  (flet ((every-string-equal (list1 list2)
           (is (eql (length list1) (length list2)))
           (loop
             :for string1 :in list1
             :for string2 :in list2
             :do (is (string-equal string1 string2)))))
    (every-string-equal
      (multiple-value-list
        (red-db-add-context (red-db) (uuid-print-urn uuid:+namespace-dns+)))
      (list "urn:uuid:6BA7B810-9DAD-11D1-80B4-00C04FD430C8"
        "context:6BA7B8109DAD11D180B400C04FD430C8"))
    (every-string-equal
      (multiple-value-list
        (red-db-find-context (red-db) (uuid-print-urn uuid:+namespace-dns+)))
      (list "urn:uuid:6BA7B810-9DAD-11D1-80B4-00C04FD430C8"
        "context:6BA7B8109DAD11D180B400C04FD430C8"))
    (every-string-equal
      (multiple-value-list
        (red-db-delete-context (red-db) (uuid-print-urn uuid:+namespace-dns+)))
      (list "urn:uuid:6BA7B810-9DAD-11D1-80B4-00C04FD430C8"
        "context:6BA7B8109DAD11D180B400C04FD430C8"))
    (is (not (red-db-find-context (red-db) (uuid-print-urn uuid:+namespace-dns+))))
    (is (not (red-db-delete-context (red-db) (uuid-print-urn uuid:+namespace-dns+))))))



(def test test/node-id/0 ()
  (dotimes (i 3)
    (is (equalp "6BA7B8149DAD11D180B400C04FD430C8"
          (node-id uuid:+namespace-x500+)))
    (is (equalp "6BA7B8149DAD11D180B400C04FD430C8"
          (node-id (uri-namestring uuid:+namespace-x500+))))
    (is (equalp "C316495DFDD45DAD9B1DB005F540542D"
          (node-id "http://example.com/")))
    (is (equalp "C316495DFDD45DAD9B1DB005F540542D"
          (node-id (puri:uri "http://example.com/"))))
    (is (equalp "7DFFA80C71F95335823CDC0406978148"
          (node-id w:-rdf-type-uri-)))
    (is (equalp "7DFFA80C71F95335823CDC0406978148"
          (node-id (puri:uri w:-rdf-type-uri-))))
    (is (equalp "7DFFA80C71F95335823CDC0406978148"
          (node-id '|rdf|:|type|)))
    (is (equalp "64D3E8A3DE475DF0B75D121895C5D54F"
          (node-id (w::db-make-literal (rxi::wilbur-db) "5" :datatype !xsd:integer))))
    (is (equalp "9CB259832FD0503CA77DEA099EA76387"
          (node-id (w::db-make-literal (rxi::wilbur-db) "take literally" :datatype !xsd:string))))))


(def test test/node-key/0 ()
  (dotimes (i 3)
    (is (equalp "uuid:6BA7B8129DAD11D180B400C04FD430C8"
          (node-key uuid:+namespace-oid+)))
    (is (equalp "uuid:6BA7B8129DAD11D180B400C04FD430C8"
          (node-key (uri-namestring uuid:+namespace-oid+))))    
    (is (equalp "uuid:6BA7B8149DAD11D180B400C04FD430C8"
          (node-key uuid:+namespace-x500+)))
    (is (equalp "uuid:6BA7B8149DAD11D180B400C04FD430C8"
          (node-key (uri-namestring uuid:+namespace-x500+))))))

(def test test/node-key/1 ()
  (dotimes (i 3)
    (is (equalp "literal:64D3E8A3DE475DF0B75D121895C5D54F"
          (node-key (w::db-make-literal (rxi::wilbur-db) "5"
                      :datatype !xsd:integer))))
    (is (equalp "literal:9CB259832FD0503CA77DEA099EA76387"
          (node-key (w::db-make-literal (rxi::wilbur-db) "take literally"
                      :datatype !xsd:string))))
    (is (equalp "node:C316495DFDD45DAD9B1DB005F540542D"
          (node-key "http://example.com/")))
    (is (equalp "node:C316495DFDD45DAD9B1DB005F540542D"
          (node-key (puri:uri "http://example.com/"))))
    (is (equalp "node:7DFFA80C71F95335823CDC0406978148"
          (node-key w:-rdf-type-uri-)))
    (is (equalp "node:7DFFA80C71F95335823CDC0406978148"
          (node-key (puri:uri w:-rdf-type-uri-))))
    (is (equalp "node:7DFFA80C71F95335823CDC0406978148"
          (node-key '|rdf|:|type|)))))



(def test test/red-db-*-node/uuid/0 ()
  (is (equalp
        (list "uuid:6BA7B8149DAD11D180B400C04FD430C8"
          "urn:uuid:6ba7b814-9dad-11d1-80b4-00c04fd430c8")
        (multiple-value-list
          (red-db-add-node (red-db) uuid:+namespace-x500+))))
  (is (equalp
        (list "urn:uuid:6ba7b814-9dad-11d1-80b4-00c04fd430c8"
          "uuid:6BA7B8149DAD11D180B400C04FD430C8")
        (multiple-value-list
          (red-db-find-node (red-db) (node-key uuid:+namespace-x500+)))))
  (is (equalp
        (list "urn:uuid:6ba7b814-9dad-11d1-80b4-00c04fd430c8"
          "uuid:6BA7B8149DAD11D180B400C04FD430C8")
        (multiple-value-list
          (red-db-find-node (red-db) uuid:+namespace-x500+))))
  (is (equalp
        (list "uuid:6BA7B8149DAD11D180B400C04FD430C8"
          "urn:uuid:6ba7b814-9dad-11d1-80b4-00c04fd430c8")
        (multiple-value-list
          (red-db-delete-node (red-db) uuid:+namespace-x500+))))
  (is (not
        (red-db-find-node (red-db) (node-key uuid:+namespace-x500+))))
  (is (equalp
        (list "uuid:6BA7B8129DAD11D180B400C04FD430C8"
          "urn:uuid:6ba7b812-9dad-11d1-80b4-00c04fd430c8")
        (multiple-value-list
          (red-db-add-node (red-db) uuid:+namespace-oid+))))
  (is (equalp
        (list "urn:uuid:6ba7b812-9dad-11d1-80b4-00c04fd430c8"
          "uuid:6BA7B8129DAD11D180B400C04FD430C8")
        (multiple-value-list
          (red-db-find-node (red-db) uuid:+namespace-oid+))))
  (is (equalp
        (list "urn:uuid:6ba7b812-9dad-11d1-80b4-00c04fd430c8"
          "uuid:6BA7B8129DAD11D180B400C04FD430C8")
        (multiple-value-list
          (red-db-find-node (red-db) (node-key uuid:+namespace-oid+)))))
  (is (equalp
        (list "uuid:6BA7B8129DAD11D180B400C04FD430C8"
          "urn:uuid:6ba7b812-9dad-11d1-80b4-00c04fd430c8")
        (multiple-value-list
          (red-db-delete-node (red-db) (node-key uuid:+namespace-oid+)))))
  (is (not (red-db-delete-node (red-db) uuid:+namespace-x500+))))



(def test test/red-db-*-node/node/0 ()
  (is (equalp
        (multiple-value-list
          (red-db-add-node (red-db) (puri:uri "http://example.com/")))
        (list "node:C316495DFDD45DAD9B1DB005F540542D" "http://example.com/")))
  (is (equalp
        (multiple-value-list
          (red-db-find-node (red-db) (puri:uri "http://example.com/")))
        (list "http://example.com/" "node:C316495DFDD45DAD9B1DB005F540542D")))
  (is (equalp
        (multiple-value-list
          (red-db-find-node (red-db) (node-key "http://example.com/")))
        (list "http://example.com/" "node:C316495DFDD45DAD9B1DB005F540542D")))
  (is (equalp
        (multiple-value-list
          (red-db-find-node (red-db) "http://example.com/"))
        (list "http://example.com/" "node:C316495DFDD45DAD9B1DB005F540542D")))
  (is (equalp
        (multiple-value-list
          (red-db-delete-node (red-db) "http://example.com/"))
        (list "node:C316495DFDD45DAD9B1DB005F540542D" "http://example.com/")))
  (is (not (red-db-delete-node (red-db) "http://example.com/"))))

(def test test/red-db-*-node/literal/0 ()
  (is (equalp
        (multiple-value-list
          (red-db-add-node (red-db) (w:literal "zen koans of the recursive minibuffer"
                                                :datatype !xsd:string)))
        (list "literal:054F203E6C005B85A621E87B940276F4"
          "#\"zen koans of the recursive minibuffer\"^^<xsd:string>")))
  (is (equalp
        (multiple-value-list
          (red-db-find-node (red-db) "literal:054F203E6C005B85A621E87B940276F4"))
        (list (w:literal "zen koans of the recursive minibuffer" :datatype !xsd:string) 
           "literal:054F203E6C005B85A621E87B940276F4")))
  (is (equalp
        (multiple-value-list
          (red-db-delete-node (red-db) "literal:054F203E6C005B85A621E87B940276F4"))
        (list "literal:054F203E6C005B85A621E87B940276F4"
          "#\"zen koans of the recursive minibuffer\"^^<xsd:string>")))
  (is (not
        (red-db-find-node (red-db) "literal:054F203E6C005B85A621E87B940276F4"))))


(def test test/red-db-*-node/literal/1 ()
  (is (equalp
        (multiple-value-list
          (red-db-add-node (red-db) (w:literal "5" :datatype !xsd:integer)))
        (list "literal:64D3E8A3DE475DF0B75D121895C5D54F" "#\"5\"^^<xsd:integer>")))
  (is (equalp
        (multiple-value-list                                      
          (red-db-find-node (red-db) (w:literal "5" :datatype !xsd:integer)))
        (list (w:literal "5" :datatype !xsd:integer) "literal:64D3E8A3DE475DF0B75D121895C5D54F")))
  (is (equalp
        (multiple-value-list                                      
          (red-db-find-node (red-db) "literal:64D3E8A3DE475DF0B75D121895C5D54F"))
        (list (w:literal "5" :datatype !xsd:integer) "literal:64D3E8A3DE475DF0B75D121895C5D54F")))
  (is (equalp
        (multiple-value-list                                      
          (red-db-delete-node (red-db) (w:literal "5" :datatype !xsd:integer)))
        (list "literal:5382C9C582BA55D8A9EA0FCA8745C46A" "#\"5\"^^<xsd:integer>")))
  (is (not
        (red-db-delete-node (red-db) (w:literal "5" :datatype !xsd:integer)))))


(def test test/spoc-id/0 ()
  (is (equalp (spoc-id "s" "p" "o" "c") "afb288afd4cc6985e05af500c0245ef858e3eb92"))
  (is (equalp "f77ecbe1c5f7035654d70c61702918c407053b23"
        (spoc-id
          (uuid:make-null-uuid)
          "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
          (puri:uri "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"))))
  (is (equalp "f77ecbe1c5f7035654d70c61702918c407053b23"
        (spoc-id
          (uuid:make-null-uuid)
          '|rdf|:|type|
          (puri:uri "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"))))
  (is (equalp "f77ecbe1c5f7035654d70c61702918c407053b23"
        (spoc-id
          (uri-namestring (uuid:make-null-uuid))
          w:-rdf-type-uri-
          "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property")))
  (is (equalp "f77ecbe1c5f7035654d70c61702918c407053b23"
        (spoc-id
          (uuid-print-urn (uuid:make-null-uuid))
          (puri:uri w:-rdf-type-uri-)
          (puri:uri "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"))))
  (is (equalp "f77ecbe1c5f7035654d70c61702918c407053b23"
        (spoc-id
          (uuid:make-null-uuid)
          (puri:uri w:-rdf-type-uri-)
          '|rdf|:|Property|)))
  (is (equalp "f77ecbe1c5f7035654d70c61702918c407053b23"
        (spoc-id
          (uuid:make-null-uuid)
          '|rdf|:|type|
          '|rdf|:|Property|))))



(def test test/print-spoc-key/0 ()
  (is (equalp (print-spoc-key "x" "y") "spoc:x:y")))



(def test test/spoc-key/0 ()
  (is (equalp (spoc-key "s" "p" "o" "c")
        "spoc:afb288afd4cc6985e05af500c0245ef858e3eb92:D61EFC485B8756169FFD04412C02223A"))
  (is (equalp
        "spoc:f77ecbe1c5f7035654d70c61702918c407053b23:00000000000000000000000000000000"
        (spoc-key
          (uuid:make-null-uuid)
          "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
          (puri:uri "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"))))
  (is (equalp
        "spoc:f77ecbe1c5f7035654d70c61702918c407053b23:00000000000000000000000000000000"
        (spoc-key
          (uuid:make-null-uuid)
          '|rdf|:|type|
          '|rdf|:|Property|)))
  (is (equalp
        "spoc:f77ecbe1c5f7035654d70c61702918c407053b23:00000000000000000000000000000000"
        (spoc-key
          (uuid:make-null-uuid)
          (puri:uri w:-rdf-type-uri-)
          '|rdf|:|Property|))))


(def test test/s-key/0 ()
  (is (equalp "s:A2345387DB8C5083A0F4B6B35025BC6B:00000000000000000000000000000000"
        (s-key '|rdfs|:|Resource|)))
  (is (equalp "s:A2345387DB8C5083A0F4B6B35025BC6B:00000000000000000000000000000000"
        (s-key w:-rdfs-resource-uri-))))



(def test test/p-key/0 ()
  (is (equalp "p:D1F8784BAF5B5BA08BA6716EBD2E2473:00000000000000000000000000000000"
        (p-key '|rdf|:|subject|)))
  (is (equalp "p:D1F8784BAF5B5BA08BA6716EBD2E2473:00000000000000000000000000000000"
        (p-key w:-rdf-subject-uri-))))

(def test test/o-key/0 ()
  (is (equalp "o:50B7F8C323F257C38CD8E13029C8F6D6:00000000000000000000000000000000"
        (o-key '|rdfs|:|Class|)))
  (is (equalp "o:50B7F8C323F257C38CD8E13029C8F6D6:00000000000000000000000000000000"
        (o-key w:-rdfs-class-uri-))))

(def test test/po-key/0 ()
  (is (equalp "po:e144118703fad8dd28c86c79746f5f22cf82dfeb:00000000000000000000000000000000"
        (po-key '|rdf|:|type| '|rdfs|:|Class|)))
  (is (equalp "po:e144118703fad8dd28c86c79746f5f22cf82dfeb:00000000000000000000000000000000"
        (po-key w:-rdf-type-uri- w:-rdfs-class-uri-))))


(def test test/so-key/0 ()
  (is (equalp "so:e862c9205ca64adc312a24ff877934b346c8e534:00000000000000000000000000000000"
        (so-key '|rdf|:|Bag| '|rdfs|:|Class|)))
  (is (equalp "so:e862c9205ca64adc312a24ff877934b346c8e534:00000000000000000000000000000000"
        (so-key w:-rdf-bag-uri- w:-rdfs-class-uri-))))



(def test test/sp-key/0 ()
  (is (equalp "sp:4537e23b34ec841ce11315396c3a5e2aa6795247:00000000000000000000000000000000"
        (sp-key '|rdf|:|Bag| '|rdf|:|type|)))
  (is (equalp "sp:4537e23b34ec841ce11315396c3a5e2aa6795247:00000000000000000000000000000000"
        (sp-key w:-rdf-bag-uri- w:-rdf-type-uri-))))


;;;;;
;; Local Variables:
;; indent-tabs: nil
;; outline-regexp: ";;[;]+"
;; End:
;;;;;
