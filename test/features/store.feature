Feature: Storing data
    In order for pyrakoon to be useful
    It should be able to store data

    Scenario: Setting and testing a key
        Given I am connected to Arakoon
        When I set "foo" to "bar"
        And I check whether "foo" exists
        Then it should be found

    Scenario: Testing a non-existing key
        Given I am connected to Arakoon
        When I check whether "foo" exists
        Then it should not be found

    Scenario: Retrieving a value
        Given I am connected to Arakoon
        And I set "foo" to "bar"
        When I retrieve "foo"
        Then "bar" should be returned

    Scenario: Retrieving a non-existing value
        Given I am connected to Arakoon
        When I retrieve "foo"
        Then a NotFound('foo',) exception is raised

    Scenario: Updating a value
        Given I am connected to Arakoon
        And I set "foo" to "bar"
        When I set "foo" to "baz"
        And I retrieve "foo"
        Then "baz" should be returned

    Scenario: Deleting a key
        Given I am connected to Arakoon
        And I set "foo" to "bar"
        When I delete "foo"
        And I check whether "foo" exists
        Then it should not be found

    Scenario: Deleting a non-existing key
        Given I am connected to Arakoon
        When I delete "foo"
        Then a NotFound('foo',) exception is raised

    Scenario: Retrieving prefixed keys
        Given I am connected to Arakoon
        And I create 10 keys starting with "foo_"
        And I create 10 keys starting with "bar_"
        When I retrieve all keys starting with "foo_"
        Then 10 keys should be returned

    Scenario: Retrieving a limited set of prefixed keys
        Given I am connected to Arakoon
        And I create 20 keys starting with "foo_"
        When I retrieve 10 keys starting with "foo_"
        Then 10 keys should be returned

    Scenario: Retrieving a limited set of prefixed keys, but less available
        Given I am connected to Arakoon
        And I create 5 keys starting with "foo_"
        When I retrieve 10 keys starting with "foo_"
        Then 5 keys should be returned
