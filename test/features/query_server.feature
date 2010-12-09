Feature: Query the server
    In order for pyrakoon to be a good client
    It should be able to query the server

    Scenario: Saying hello
        Given I am connected to Arakoon
        When I say hello
        Then I receive the correct version string

    Scenario: Querying master node
        Given I am connected to Arakoon
        When I request the master node name
        Then I receive the correct master node name
