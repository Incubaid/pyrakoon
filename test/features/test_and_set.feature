Feature: Test and set
    In order for pyrakoon to be useful
    It should support "test_and_set"

    Scenario: Test and set an unset key using None as original value
        Given I am connected to Arakoon
        When I test_and_set "foo" from None to "bar"
        Then None should be returned

    Scenario: Test and set an unset key using None as original value, and retrieve the new value
        Given I am connected to Arakoon
        When I test_and_set "foo" from None to "bar"
        And I retrieve "foo"
        Then "bar" should be returned

    Scenario: Test and set a set key using the original value
        Given I am connected to Arakoon
        And I set "foo" to "bar"
        When I test_and_set "foo" from "bar" to "baz"
        Then "bar" should be returned

    Scenario: Test and set a key using the original value, and retrieve the new value
        Given I am connected to Arakoon
        And I set "foo" to "bar"
        When I test_and_set "foo" from "bar" to "baz"
        And I retrieve "foo"
        Then "baz" should be returned

    Scenario: Test and set a key using a wrong value
        Given I am connected to Arakoon
        And I set "foo" to "bar"
        When I test_and_set "foo" from "baz" to "bat"
        And I retrieve "foo"
        Then "bar" should be returned
